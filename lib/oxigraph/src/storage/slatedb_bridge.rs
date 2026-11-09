//! Bridge between SlateDB's asynchronous API and Oxigraph's synchronous storage layer.

use crate::storage::error::StorageError;
use std::future::Future;
use tokio::runtime::{Handle, Runtime};

/// A private Tokio runtime that runs SlateDB futures on behalf of synchronous code.
///
/// The calling thread never drives the future itself: it spawns the future on this runtime and
/// blocks on a [`std::sync::mpsc`] channel. This is what makes the bridge safe when Oxigraph is
/// embedded in an application that already runs a Tokio runtime, where `Runtime::block_on` would
/// panic ("cannot block the current thread from within a runtime").
///
/// Only std channels may cross the async/sync boundary here: `Runtime::block_on`, `Handle::block_on`
/// and `tokio::sync::mpsc::Receiver::blocking_recv` are all forbidden in this module.
pub(crate) struct RuntimeBridge {
    handle: Handle,
    // Declared after `handle`, and taken by `drop`, so the runtime outlives every handle and never
    // joins its workers on the thread that drops it.
    runtime: Option<Runtime>,
}

impl Drop for RuntimeBridge {
    fn drop(&mut self) {
        if let Some(runtime) = self.runtime.take() {
            // Dropping a runtime waits for its worker threads to stop, which panics when it happens
            // inside an asynchronous context, i.e. when an embedded application drops its last
            // `Store` handle from one of its own Tokio tasks. `shutdown_background` never blocks.
            runtime.shutdown_background();
        }
    }
}

impl RuntimeBridge {
    pub(crate) fn new() -> Result<Self, StorageError> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            // SlateDB keeps a few background tasks (WAL, compaction, garbage collection); two
            // workers is enough because this runtime only ever runs storage traffic.
            .worker_threads(2)
            .enable_time()
            .thread_name("oxigraph-slatedb")
            .build()?;
        let handle = runtime.handle().clone();
        Ok(Self {
            handle,
            runtime: Some(runtime),
        })
    }

    /// Runs `future` on the bridge runtime and blocks the calling thread until it completes.
    ///
    /// Returns an error if the task panicked or the runtime is shutting down, in which case the
    /// future never produced a value.
    pub(crate) fn run<F>(&self, future: F) -> Result<F::Output, StorageError>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let (sender, receiver) = std::sync::mpsc::sync_channel(1);
        self.handle.spawn(async move {
            if sender.send(future.await).is_err() {
                // The caller gave up, so there is nobody left to hand the value to.
            }
        });
        receiver
            .recv()
            .map_err(|_| StorageError::Other("the SlateDB worker task failed".into()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runs_futures_from_a_plain_thread() {
        let bridge = RuntimeBridge::new().unwrap();
        assert_eq!(bridge.run(async { 7 }).unwrap(), 7);
        let bridge = std::thread::spawn(move || bridge.run(async { 11 }).unwrap());
        assert_eq!(bridge.join().unwrap(), 11);
    }

    /// The anti-deadlock regression test: `Runtime::block_on` panics when called from inside a
    /// Tokio runtime, and a naive bridge built on it would break every embedding application.
    #[test]
    fn runs_futures_from_inside_another_tokio_runtime() {
        let caller_runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_time()
            .build()
            .unwrap();
        let bridge = RuntimeBridge::new().unwrap();
        caller_runtime.block_on(async {
            assert_eq!(bridge.run(async { 42 }).unwrap(), 42);
            // A bridge call nested in an async task, not only at the root of `block_on`.
            tokio::spawn(async move { assert_eq!(bridge.run(async { 43 }).unwrap(), 43) })
                .await
                .unwrap();
        });
    }

    #[test]
    fn reports_a_task_that_panics() {
        let bridge = RuntimeBridge::new().unwrap();
        // Divided by a zero the optimizer cannot fold away, so that the task panics at runtime
        // without going through the `panic!` macro the workspace lints forbid.
        let divisor: u8 = std::hint::black_box(0);
        let error = bridge
            .run(async move { u8::MAX / divisor })
            .err()
            .map(|error| error.to_string());
        assert_eq!(error, Some("the SlateDB worker task failed".to_owned()));
    }
}
