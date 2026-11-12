//! Bridge between SlateDB's asynchronous API and Oxigraph's synchronous storage layer.

use crate::storage::error::StorageError;
use std::future::Future;
use tokio::runtime::{Handle, Runtime};

/// A private Tokio runtime that runs SlateDB futures on behalf of synchronous code.
///
/// A caller that is not on a runtime thread drives its future with [`Handle::block_on`], which
/// avoids the spawn and the channel round trip that dominate a storage call. The guard is
/// [`Handle::try_current`]: when the caller *is* inside a runtime, blocking would panic or
/// deadlock, so the future is spawned on this runtime and the caller blocks on a
/// [`std::sync::mpsc`] channel instead. That fallback is what makes the bridge safe when Oxigraph
/// is embedded in an application that already runs a Tokio runtime, where `Runtime::block_on`
/// would panic ("cannot block the current thread from within a runtime").
///
/// `Handle::block_on` must only be called after `try_current()` proved the caller is a plain
/// thread, and `Runtime::block_on` and `tokio::sync::mpsc::Receiver::blocking_recv` stay forbidden
/// here whatever the caller is.
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
            // The S3 client of the object store opens TCP connections, and without the I/O driver
            // it panics with "IO is disabled" the first time a request is sent. The local file
            // system backend never needs it, which is why it stayed unnoticed.
            .enable_io()
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
    /// future never produced a value. `run` borrows the bridge and [`Drop`] only shuts the runtime
    /// down once the last borrow is gone, so the runtime is never shut down while it is driven here.
    pub(crate) fn run<F>(&self, future: F) -> Result<F::Output, StorageError>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        if Handle::try_current().is_ok() {
            // The caller runs on a runtime thread, where `block_on` would panic or deadlock: hand
            // the future to our workers and wait on a channel. Nested calls take this path too,
            // because `block_on` below enters this runtime's context for the future it drives.
            return self.run_on_worker(future);
        }
        // A plain thread: drive the future here, which is two orders of magnitude cheaper than a
        // spawn and a channel round trip. The runtime keeps driving its background tasks (WAL
        // flush, compaction, garbage collection) on its own workers.
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.handle.block_on(future)
        })) {
            Ok(output) => Ok(output),
            // `Handle::block_on` resumes a panic on this thread instead of turning it into a
            // closed channel; report it the way the spawned path above does. The future is dropped
            // while unwinding, exactly like a panicking spawned task, so nothing escapes.
            Err(_) => Err(worker_failure()),
        }
    }

    /// Runs `future` on a bridge worker and blocks the calling thread until it completes, without
    /// the fast path [`Self::run`] takes on a plain thread.
    ///
    /// This is for the one-shot futures of the opening path, which schedule tasks of their own and
    /// yield thousands of times while they replay the manifest and the write-ahead log: a future
    /// driven by `Handle::block_on` pays a park and an unpark of the calling thread for each of
    /// those yields. On the 100 000 quad cold open workload, opening took 351–364 ms through this
    /// path against 549–771 ms through `Handle::block_on`, while every per-operation call of the
    /// fast path measured faster (planning/slatedb/performance.md).
    pub(crate) fn run_on_worker<F>(&self, future: F) -> Result<F::Output, StorageError>
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
        receiver.recv().map_err(|_| worker_failure())
    }
}

/// The error reported when a storage task panicked or the runtime is shutting down.
fn worker_failure() -> StorageError {
    StorageError::Other("the SlateDB worker task failed".into())
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

    /// The same panic contract on the channel path, i.e. when the caller is on a runtime thread.
    #[test]
    fn reports_a_task_that_panics_inside_another_tokio_runtime() {
        let caller_runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_time()
            .build()
            .unwrap();
        let bridge = RuntimeBridge::new().unwrap();
        let divisor: u8 = std::hint::black_box(0);
        let error = caller_runtime.block_on(async move {
            bridge
                .run(async move { u8::MAX / divisor })
                .err()
                .map(|error| error.to_string())
        });
        assert_eq!(error, Some("the SlateDB worker task failed".to_owned()));
    }

    /// `Handle::block_on` only drives the future it is given, so the runtime must keep its timers
    /// running on its workers for a future that awaits one.
    #[test]
    fn runs_a_future_that_awaits_a_timer_from_a_plain_thread() {
        let bridge = RuntimeBridge::new().unwrap();
        let value = bridge
            .run(async {
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                5
            })
            .unwrap();
        assert_eq!(value, 5);
    }

    /// The S3 client of the object store opens its sockets from this runtime, and without the I/O
    /// driver the first request panics with "IO is disabled". Binding a socket is the cheapest
    /// operation that needs the driver.
    #[test]
    fn runs_a_future_that_opens_a_socket() {
        let bridge = RuntimeBridge::new().unwrap();
        let port = bridge
            .run(async {
                let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
                    .await
                    .unwrap();
                listener.local_addr().unwrap().port()
            })
            .unwrap();
        assert_ne!(port, 0);
    }

    #[test]
    fn runs_futures_on_a_worker_from_a_plain_thread() {
        let bridge = RuntimeBridge::new().unwrap();
        assert_eq!(bridge.run_on_worker(async { 7 }).unwrap(), 7);
        let divisor: u8 = std::hint::black_box(0);
        let error = bridge
            .run_on_worker(async move { u8::MAX / divisor })
            .err()
            .map(|error| error.to_string());
        assert_eq!(error, Some("the SlateDB worker task failed".to_owned()));
    }

    /// If this ever fails, every call from a plain thread silently takes the slow path.
    #[test]
    #[expect(
        clippy::assertions_on_result_states,
        reason = "the assertion is the point of the test"
    )]
    fn leaves_no_runtime_context_on_the_caller() {
        let bridge = RuntimeBridge::new().unwrap();
        bridge.run(async {}).unwrap();
        assert!(Handle::try_current().is_err());
    }
}
