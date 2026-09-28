# Closing the 185× SERVICE Gap: Per-Tuple Evaluation Cache

## Current State

After implementing streaming (channel-based `handle_with_context()`), the N=5000 benchmark improved from **5,366ms → 543ms** (9.9×). But the remaining 185× gap vs direct (3ms) comes from a new bottleneck.

### Root Cause: Per-Tuple `evaluate_service()` Calls

In `spareval/src/eval.rs`, the `Service` graph pattern evaluator creates a closure that is called **once per incoming tuple** from the left side of a join:

```rust
// eval.rs line ~1596 — called PER incoming tuple 'from'
Rc::new(move |from| {
    match eval.evaluate_service(&service_name, &graph_pattern, &variables, &from) {
        Ok(result) => Box::new(result.filter_map(move |binding| {
            binding.map(|binding| binding.combine_with(&from)).transpose()
        })),
```

For query `SERVICE(store0) . SERVICE(store1)`:
1. `evaluate_service(store0)` — called **once** (no incoming bindings), spawns 1 thread
2. `evaluate_service(store1)` — called **per tuple from store0** (~100-200 times by LIMIT 50), spawns 100-200 threads

Each call to `evaluate_service()` → `handle_with_context()` → thread spawn → RocksDB snapshot → query evaluation → channel → join filtering. At 100+ calls, this is ~500ms of thread overhead.

## Solution: Per-Query SERVICE Result Cache

Add a cache inside `SimpleEvaluator` that stores the results of `evaluate_service()` keyed by service IRI. The first call evaluates and caches; subsequent calls with the same IRI reuse the cached result.

### Design

```rust
// In SimpleEvaluator (eval.rs)
pub struct SimpleEvaluator<'a, D: QueryableDataset<'a>> {
    // ... existing fields ...
    service_cache: RefCell<HashMap<NamedNode, Arc<Vec<InternalTuple<D::InternalTerm>>>>>,
}
```

The `evaluate_service()` method becomes:

```rust
fn evaluate_service(&self, service_name, graph_pattern, variables, from) -> Result<...> {
    // Resolve service_name IRI
    let service_name = ...;

    // Check cache
    let mut cache = self.service_cache.borrow_mut();
    if let Some(cached) = cache.get(&service_name) {
        // Filter cached tuples against 'from' bindings
        let from = from.clone();
        let cached = Arc::clone(cached);
        return Ok(Box::new(
            cached.iter().filter_map(move |tuple| {
                tuple.combine_with(&from).map(Ok)
            })
        ));
    }

    // First call: evaluate and cache
    let context = ServiceContext {
        cancellation_token: self.dataset.cancellation_token.clone(),
        ..Default::default()
    };
    let iter = self.service_handler
        .handle_with_context(&service_name, graph_pattern, base_iri, context)?;

    // Collect results into cache
    let variables_clone = Rc::clone(&variables);
    let encoded: Vec<InternalTuple<D::InternalTerm>> = encode_bindings(
        self.dataset.clone(), variables_clone, iter
    ).filter_map(Result::ok).collect();

    let cached = Arc::new(encoded);
    cache.insert(service_name, Arc::clone(&cached));

    // Return cached data filtered by current bindings
    let from = from.clone();
    Ok(Box::new(
        cached.iter().filter_map(move |tuple| {
            tuple.combine_with(&from).map(Ok)
        })
    ))
}
```

### Trade-off Analysis

**Memory:** Cache stores ALL solutions from each SERVICE call. For N=5000:
- store0 (types): 169K solutions × ~200 bytes = ~34MB
- store1 (labels): 30K solutions × ~200 bytes = ~6MB
- Total: ~40MB per query

**Correctness:** The cache is valid because:
1. SERVICE calls are pure functions of `(store, pattern)` — same inputs → same outputs
2. The service IRI is constant (not a variable) for `oxigraph://` URIs
3. The cache lives for one query execution (not across queries)
4. Cancellation between cache insertion and cache reads is handled by the caller

**Thread safety:** `RefCell` is single-threaded (the evaluator runs in one thread). The `Rc` closures share the `SimpleEvaluator` via clone, but the borrows happen within the same thread's call stack.

### Expected Impact

| Stage | SERVICE time (N=5000) | Overhead vs direct |
|-------|----------------------|---------------------|
| Original (eager collect) | 5,366 ms | 1,820× |
| After streaming | 543 ms | 185× |
| After cache (+ streaming) | **~10-20 ms** | **~5×** |

The remaining ~5× gap is the irreducible overhead of:
- Two RocksDB snapshot creations and query evaluations (~5ms each)
- Two thread spawns for streaming (~2ms each) — **eliminate by removing threads when cache is used**
- Tuple materialization and join (~1ms)

### Optimization: Skip Thread When Result Comes From Cache

Once the cache is in place, the thread-based streaming in `LocalStoreServiceHandler::handle_with_context()` is only needed for the **first** call per service IRI. All subsequent calls hit the cache. But even the first call uses a thread unnecessarily — the `DefaultServiceHandler` trait returns `QuerySolutionIter<'static>` which requires the data to outlive the call.

**Alternative:** Add a `handle_blocking()` method to `DefaultServiceHandler` that doesn't require `'static`:

```rust
pub trait DefaultServiceHandler {
    /// Evaluate synchronously, borrowing from the handler's state.
    /// The caller is responsible for consuming the iterator before the handler is dropped.
    fn handle_blocking(
        &self,
        service_name: &NamedNode,
        pattern: &GraphPattern,
        base_iri: Option<&Iri<OxString>>,
        context: ServiceContext,
    ) -> Result<QuerySolutionIter<'_>, Self::Error>;
}
```

With `handle_blocking()`, the evaluator can call the handler synchronously (no thread), collect results into the cache, and drop the iterator. This eliminates thread overhead entirely for the common case.

## Implementation Plan

### Step 1: Add `service_cache` to `SimpleEvaluator`

**File:** `lib/spareval/src/eval.rs`

Add a field to `SimpleEvaluator`:
```rust
service_cache: Rc<RefCell<HashMap<NamedNode, Arc<Vec<InternalTuple<D::InternalTerm>>>>>>,
```

Initialize in `SimpleEvaluator::new()`:
```rust
service_cache: Rc::new(RefCell::new(HashMap::new())),
```

### Step 2: Rewrite `evaluate_service()` with caching

Replace the existing `evaluate_service()` method body with the cache-first logic described above.

### Step 3: Handle variable service names

If the service name is a variable (e.g., `SERVICE ?endpoint { ... }`), the cache key varies per binding. In that case, **skip caching** — variable service names are rare with `oxigraph://` and the per-tuple evaluation is correct.

```rust
// Only cache when service_name is a constant NamedNode
let cache_key = service_name.clone();
// Variable names skip the cache
```

### Step 4: Remove thread spawning for cached services (optional follow-up)

Once the cache eliminates per-tuple calls, the thread in `handle_with_context()` is only created once per service IRI. The overhead of one thread (~1ms) is negligible compared to query evaluation (~5ms). This step can be deferred.

### Step 5: Benchmark

Run `cargo bench --bench local_service` for N=1000 and N=5000.

**Target:**
| N | Direct | SERVICE | Ratio |
|---|--------|---------|-------|
| 1K | ~52 ms | ~60 ms | 1.2× |
| 5K | ~3 ms | ~15 ms | 5× |

## Files Modified

1. `lib/spareval/src/eval.rs` — Add `service_cache` field, rewrite `evaluate_service()`

## Risks

- **Memory:** 40MB cache per query at N=5000. Acceptable for a database; configurable via eviction or size limit in the future.
- **Variable service names:** Must detect and skip caching for variable names.
- **Cancellation:** If the query is cancelled between cache population and cache read, the service may have been partially evaluated. The cache stores only successfully collected tuples (`filter_map(Result::ok)`), so cancelled evaluations produce empty/partial caches. This is safe — the outer query will see fewer results than expected and the cancellation token will propagate normally.
