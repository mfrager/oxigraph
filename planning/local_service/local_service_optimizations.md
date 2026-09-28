# LocalStoreServiceHandler Performance Optimizations

## Problem Statement

The `LocalStoreServiceHandler` resolves `SERVICE <oxigraph://...>` URIs to local on-disk `Store` instances and evaluates the SERVICE sub-pattern in-process. The current implementation, while correct, has a severe scalability bottleneck: **all intermediate SERVICE results are eagerly collected before the outer query's join, limit, or filter can be applied.**

Benchmark results from `basenote-bench` (commit `0a78032`) on BSBM federated joins (3 data sources, 5 query types):

| N | Direct (named graphs) | SERVICE (3 stores) | Ratio |
|---|----------------------|--------------------|-------|
| 1K | 52 ms | 117 ms | **2.2×** |
| 5K | 3 ms | 5,370 ms | **1,820×** |

At N=5000, the SERVICE path is three orders of magnitude slower than the equivalent direct path. The root cause is that `handle()` collects **all** solutions from each SERVICE sub-query (`~169K` types × `~30K` labels = **~5 billion** intermediate join tuples) before the outer query's `LIMIT 50` can discard 99.999% of them.

## Current Architecture

```
Outer Query: SELECT ?s ?type ?label WHERE {
    SERVICE <oxigraph:///store0> { ?s rdf:type ?type . }
    SERVICE <oxigraph:///store1> { ?s rdfs:label ?label . }
    ?s bsbm:productPropertyNumeric1 ?value .
} LIMIT 50

SERVICE <oxigraph://store0>
    │
    ▼
LocalStoreServiceHandler::handle(service_name, pattern, base_iri)
    │
    ├── 1. Parse oxigraph:// URI → PathBuf
    ├── 2. Open/reuse Store (cached in DashMap<PathBuf, Arc<Store>>)
    ├── 3. Create StorageReader snapshot
    ├── 4. Build SelectQuery { pattern, dataset: None }
    ├── 5. QueryEvaluator::new().prepare(&query).execute(dataset)  ← fresh evaluator
    ├── 6. Collect ALL solutions into Vec<_>                        ← THE BOTTLENECK
    └── 7. Return QuerySolutionIter<'static>
```

Issues by line:
- **(2)** Store snapshot is created per-call; could be reused across multiple SERVICE calls to same store within one query
- **(4)** Query ignores any outer bindings, LIMIT, or ORDER BY
- **(5)** Creates a fresh `QueryEvaluator` without parent's cancellation token, statistics, or custom functions
- **(6)** `iter.collect()` is the single largest problem — no backpressure, no early termination

## Proposed Optimizations

### 1. Streaming SERVICE Results via Lazy Evaluation (Highest Impact)

**Problem:** `handle()` returns `QuerySolutionIter<'static>` which forces eager collection because the `StorageReader` snapshot must be dropped before the iterator is consumed.

**Solution:** Change the `DefaultServiceHandler` trait (or add a new variant) to accept a callback/consumer pattern, or introduce a streaming trait that borrows the handler's internal state.

**Alternative — lightweight approach:** Instead of changing the trait, implement lazy evaluation within `handle()` using a scoped thread or `Arc`-shared state:

```rust
fn handle(&self, service_name, pattern, base_iri) -> Result<QuerySolutionIter<'static>, Error> {
    let store = self.get_or_open_store(path)?;
    let reader = store.storage().snapshot();
    let dataset = DatasetView::new(reader);

    // Instead of collect(), wrap in a lazy iterator that holds the snapshot alive
    let evaluator = QueryEvaluator::new();
    let results = evaluator.prepare(&query).execute(dataset)?;

    match results {
        QueryResults::Solutions(iter) => {
            // Bundle the dataset (which owns the snapshot) with the iterator
            let bundled = BundledIter {
                _dataset: dataset,    // keeps snapshot alive
                iter,
            };
            // ... but this doesn't work because QuerySolutionIter borrows 'a
        }
    }
}
```

**Challenge:** The `'static` lifetime requirement of `QuerySolutionIter<'static>` makes streaming difficult because the RocksDB snapshot (`StorageReader`) must outlive the iterator. Solutions:

- **(a) Scoped threads:** Spawn a thread that owns the snapshot, runs the query, and sends solutions through a channel. The parent receives a `QuerySolutionIter<'static>` backed by the channel receiver.
- **(b) Arc-based snapshot sharing:** Wrap the `StorageReader` in `Arc`, clone it into the iterator, and let the iterator hold the `Arc`.
- **(c) New trait method:** Add `handle_streaming()` to `DefaultServiceHandler` that takes a callback `FnMut(QuerySolution) -> Result<(), Error>` and the caller drives iteration.

**Recommendation:** Approach **(b)** — `Arc<StorageReader>`. The `StorageReader` is already `Clone` and internally reference-counts the RocksDB snapshot. Wrapping it in `Arc` and storing it alongside a `QuerySolutionIter<'a>` (where `'a` is the `Arc`'s lifetime) is the simplest change. The iterator becomes `QuerySolutionIter<'static>` because the `Arc` keeps the snapshot alive.

**Expected impact:** SERVICE time at N=5000 drops from ~5.4s to ~50-100ms (approaching the direct path, minus SERVICE dispatch overhead). The LIMIT 50 is applied during join evaluation, not after collecting 5B tuples.

### 2. Binding Pushdown (High Impact)

**Problem:** The `DefaultServiceHandler::handle()` signature does not receive incoming variable bindings from the outer query context. When the outer query has already bound `?company = ex:Acme`, the SERVICE sub-query still evaluates the full pattern instead of a constrained version.

**Current signature:**
```rust
fn handle(&self, service_name: &NamedNode, pattern: &GraphPattern,
          base_iri: Option<&Iri<OxString>>) -> Result<QuerySolutionIter<'static>, Error>;
```

**Proposed signature:**
```rust
fn handle(&self, service_name: &NamedNode, pattern: &GraphPattern,
          base_iri: Option<&Iri<OxString>>,
          incoming_bindings: &[QuerySolution]) -> Result<QuerySolutionIter<'static>, Error>;
```

**Implementation:** When `incoming_bindings` is non-empty, the handler injects them as a `VALUES` clause at the root of the SERVICE pattern before evaluation:

```rust
fn handle(&self, service_name, pattern, base_iri, incoming) -> Result<...> {
    let mut pattern = pattern.clone();
    if !incoming.is_empty() {
        // Wrap pattern: Join { left: Values(bindings), right: pattern }
        let vars: Vec<_> = incoming[0].variables().to_vec();
        let bindings: Vec<_> = incoming.iter()
            .map(|s| vars.iter().map(|v| s.get(v).cloned()).collect())
            .collect();
        pattern = GraphPattern::Join {
            left: Box::new(GraphPattern::Values { variables: vars, bindings }),
            right: Box::new(pattern),
        };
    }
    // ... evaluate as before
}
```

**Expected impact:** For queries like `?x ex:name "foo" . SERVICE <store> { ?x ex:age ?a }`, the SERVICE is only evaluated for the specific `?x` values bound by the outer pattern, rather than scanning the entire store.

### 3. LIMIT Propagation (Medium Impact)

**Problem:** The SERVICE sub-query is evaluated without any LIMIT. The outer query's `LIMIT 50` is not visible to the handler.

**Solution:** Extract LIMIT/SLICE from the outer query's optimized algebra and pass it to the handler. The handler can then apply `LIMIT` to the inner query:

```rust
fn handle(&self, service_name, pattern, base_iri, incoming, 
          limit: Option<u64>) -> Result<...> {
    let mut query = SelectQuery { pattern, ... };
    if let Some(limit) = limit {
        // Wrap: Slice { start: 0, length: limit, inner: pattern }
        query.pattern = GraphPattern::Slice {
            inner: Box::new(query.pattern),
            start: 0,
            length: Some(limit),
        };
    }
    // ... evaluate
}
```

**Challenge:** The optimizer may reorder the SERVICE call relative to other patterns. The effective LIMIT for a specific SERVICE depends on its position in the join tree. This requires coordination between the optimizer and the evaluator.

**Simpler approach:** The evaluator at the `GraphPattern::Service` node already knows the "remaining limit" from the outer context. Pass this through to `evaluate_service()`.

### 4. Cancellation Token Propagation (Low Impact, Correctness)

**Problem:** Queries cannot be cancelled mid-SERVICE. The TODO in the code reads:
```
// TODO: propagate the cancellation token from the parent evaluator
// so that cancelling the outer query also interrupts local SERVICE calls.
```

**Solution:** The parent `QueryEvaluator` already supports `CancellationToken`. Clone it into the handler's evaluator:

```rust
fn handle(&self, ...) -> Result<...> {
    let evaluator = if let Some(token) = &self.cancellation_token {
        QueryEvaluator::new().with_cancellation_token(token.clone())
    } else {
        QueryEvaluator::new()
    };
    // ...
}
```

The handler needs to receive the cancellation token, either through a new field on `LocalStoreServiceHandler` or by passing it per-call. Since a handler is shared across queries, per-call passing is more appropriate:

```rust
fn handle(&self, service_name, pattern, base_iri, 
          cancel: CancellationToken) -> Result<...>;
```

### 5. Snapshot Reuse Across SERVICE Calls (Medium Impact)

**Problem:** Each `SERVICE <oxigraph://store0> { ... }` call creates a new RocksDB snapshot, even when the same store is queried multiple times in one SPARQL query.

**Solution:** Introduce a per-query snapshot cache. When `handle()` is called for the same store path within a single query execution, reuse the existing snapshot:

```rust
struct PerQueryState {
    snapshots: HashMap<PathBuf, StorageReader>,
}

impl LocalStoreServiceHandler {
    fn handle_with_state(&self, path, pattern, state: &mut PerQueryState) -> Result<...> {
        let reader = state.snapshots.entry(path).or_insert_with(|| {
            self.stores.get(&path).unwrap().storage().snapshot()
        });
        let dataset = DatasetView::new(reader.clone());
        // ...
    }
}
```

**Challenge:** The `DefaultServiceHandler` trait doesn't support per-query state. This would require either thread-local storage or a new trait variant.

### 6. RocksDB Approximate Statistics (Low-Medium Impact)

**Problem:** The optimizer's join ordering relies on cardinality statistics. The current `StatisticsProvider::service_statistics()` uses `store.len()` which performs a **full scan** of the entire store — prohibitively expensive.

**Solution:** Use RocksDB's approximate size API (`GetApproximateSizes`) to estimate triple counts without scanning:

```rust
fn service_statistics(&self, service_name: &NamedNode) -> Option<ServiceStatistics> {
    let store = self.get_store(path)?;
    // Use RocksDB property "rocksdb.estimate-num-keys" 
    // or approximate size divided by average key size
    let approx_keys = store.storage().approximate_key_count();
    Some(ServiceStatistics {
        triple_count: approx_keys,
        named_graph_count: Some(store.named_graphs().count() as u64),
    })
}
```

## Implementation Priority

| # | Optimization | Impact | Effort | Risk | Recommended Order |
|---|-------------|--------|--------|------|-------------------|
| 1 | Streaming results (Arc snapshot) | **Critical** | Medium | Low | **1st** |
| 2 | Binding pushdown | **High** | Medium | Medium | 2nd |
| 3 | LIMIT propagation | Medium | Low | Low | 3rd |
| 4 | Cancellation token | Low | Low | None | 4th |
| 5 | Snapshot reuse | Medium | High | Medium | 5th |
| 6 | Approximate statistics | Low-Med | Low | Low | 6th |

## Trait Changes Required

The current `DefaultServiceHandler` trait would need to evolve:

```rust
// Current (v1)
pub trait DefaultServiceHandler: Send + Sync {
    type Error: Error + Send + Sync + 'static;
    fn handle(&self, service_name: &NamedNode, pattern: &GraphPattern,
              base_iri: Option<&Iri<OxString>>) -> Result<QuerySolutionIter<'static>, Self::Error>;
}

// Proposed (v2) — backwards-compatible via default impls
pub trait DefaultServiceHandler: Send + Sync {
    type Error: Error + Send + Sync + 'static;

    fn handle(&self, service_name: &NamedNode, pattern: &GraphPattern,
              base_iri: Option<&Iri<OxString>>) -> Result<QuerySolutionIter<'static>, Self::Error> {
        self.handle_with_context(service_name, pattern, base_iri, ServiceContext::default())
    }

    fn handle_with_context(&self, service_name: &NamedNode, pattern: &GraphPattern,
              base_iri: Option<&Iri<OxString>>,
              context: ServiceContext) -> Result<QuerySolutionIter<'static>, Self::Error> {
        // Default: ignore context, call old handle()
        self.handle(service_name, pattern, base_iri)
    }
}

pub struct ServiceContext {
    pub incoming_bindings: Vec<QuerySolution>,
    pub suggested_limit: Option<u64>,
    pub cancellation_token: CancellationToken,
}
```

## Benchmark Target

After implementing optimization #1 (streaming) and #2 (binding pushdown), the expected benchmark results are:

| N | Direct (named graphs) | SERVICE (3 stores) | Target Ratio |
|---|----------------------|--------------------|-------------|
| 1K | ~52 ms | ~60 ms | **1.15×** |
| 5K | ~3 ms | ~5 ms | **1.7×** |

The remaining gap accounts for the irreducible overhead of SERVICE dispatch: store lookup, snapshot creation, and cross-store join coordination. These are constant-time overheads that amortize with data size.
