# Local Store SERVICE: Statistics & Optimization Design

## Overview

This document describes practical optimizations that become possible when
`SERVICE` calls target an in-process Oxigraph `Store` rather than a remote
HTTP endpoint.  Since the local store is directly accessible, the query
optimizer can consult real cardinalities instead of relying solely on
heuristic estimates, and the evaluator can apply execution strategies
that are impossible over HTTP.

The implementations are described in three tiers, ordered by effort vs.
benefit.

---

## Current State (Baseline)

### How the optimizer sees SERVICE today

The `sparopt::Optimizer` pipeline (`normalize → push_graph → reorder_joins →
push_filters`) handles `Service` nodes as follows:

| Phase | `Service` behaviour | Source |
|---|---|---|
| `normalize_pattern` | Pass-through; inner pattern left to remote endpoint | `optimizer.rs:156` |
| `push_graph` | Pushes GRAPH into the SERVICE inner pattern | `optimizer.rs:722` |
| `reorder_joins` | **SERVICE children participate in outer join reordering**, but inner pattern is not reordered | `optimizer.rs:991-992` |
| `push_filters` | Filters are NOT pushed into SERVICE; they wrap it instead | `optimizer.rs:470-473` |
| `estimate_graph_pattern_size` | Falls through to `estimate_graph_pattern_size(inner, ...)` — same hardcoded heuristics as local patterns | `optimizer.rs:1282` |

The key insight: when a `Join` node contains `Service` children, the join
flattening in `reorder_joins` (line 733–760) extracts them into `to_reorder`
and subjects them to greedy ordering.  **SERVICE nodes already participate
in join ordering.**  The problem is that their cardinalities come from
heuristics rather than reality.

### Current cost model (hardcoded)

```rust
fn estimate_triple_pattern_size(subject_bound, predicate_bound, object_bound) -> u64 {
    match (true, true, true) => 1,
           (true, true, false) => 10,
           (true, false, true) => 2,
           (false, true, true) => 1_000,
           (true, false, false) => 100,
           (false, false, false) => 1_000_000_000,
           (false, true, false) => 1_000_000,
           (false, false, true) => 10_000,
}
```

The `rdf:type` bonus (`saturating_add(1)`) is the only predicate-specific
adjustment.

---

## Tier 1: Cardinality Statistics (Medium Effort, High Impact)

### Goal

Replace heuristic SERVICE cardinality estimates with real numbers from the
target `Store`, so the greedy join-orderer makes better decisions.

### Architecture

#### Step 1a: A statistics trait in `sparopt`

Add to `lib/sparopt/src/optimizer.rs` (or a new `statistics.rs`):

```rust
/// Statistics about a remote (or local) SERVICE endpoint.
#[derive(Clone, Debug, Default)]
pub struct ServiceStatistics {
    /// Estimated total number of triples in the store.
    /// `None` means "unknown".
    pub triple_count: Option<u64>,
    /// Estimated number of named graphs.
    pub named_graph_count: Option<u64>,
}

/// Provider of cardinality estimates for SPARQL SERVICE endpoints.
/// Implemented by storage backends that want to influence the optimizer.
pub trait StatisticsProvider: Send + Sync {
    /// Return statistics for the SERVICE identified by `service_name`,
    /// or `None` if the provider has no information about this endpoint.
    fn service_statistics(&self, service_name: &NamedNode)
        -> Option<ServiceStatistics>;
}
```

`sparopt` currently depends on `spargebra`, `oxrdf`, `rand`.  The new trait
only needs `NamedNode` from `oxrdf` and `Option<u64>` values — no new
dependencies.

#### Step 1b: Thread `StatisticsProvider` into the Optimizer

Add an overload or refactor `Optimizer` from a unit struct to one that can
hold an optional provider:

```rust
pub struct Optimizer {
    stats: Option<Arc<dyn StatisticsProvider>>,
}

impl Optimizer {
    pub fn new() -> Self { Self { stats: None } }
    pub fn with_statistics(stats: Arc<dyn StatisticsProvider>) -> Self { ... }

    pub fn optimize_graph_pattern(&self, pattern: GraphPattern) -> GraphPattern {
        // ... uses self.stats in estimate_graph_pattern_size
    }
}
```

The only function that changes: `estimate_graph_pattern_size`.

```rust
GraphPattern::Service { name, inner, .. } => {
    if let NamedNodePattern::NamedNode(service_name) = name {
        if let Some(stats) = &self.stats {
            if let Some(service_stats) = stats.service_statistics(service_name) {
                if let Some(triple_count) = service_stats.triple_count {
                    return triple_count;
                }
            }
        }
    }
    estimate_graph_pattern_size(inner, input_types) // fallback
}
```

**Fallback is identical to current behavior** — if no provider, or the
provider doesn't know about a particular service, the heuristic estimate
is used unchanged.

#### Step 1c: Implement `StatisticsProvider` for `LocalStoreServiceHandler`

In `lib/oxigraph/src/sparql/local_service.rs`:

```rust
use sparopt::statistics::{ServiceStatistics, StatisticsProvider};

impl StatisticsProvider for LocalStoreServiceHandler {
    fn service_statistics(&self, service_name: &NamedNode)
        -> Option<ServiceStatistics>
    {
        let iri = service_name.as_str();
        let path_str = iri.strip_prefix("oxigraph://")?;
        let path = PathBuf::from(path_str);

        // Use the already-cached store (or try to open read-only)
        let store = match self.stores.entry(path) {
            Entry::Occupied(e) => Arc::clone(e.get()),
            Entry::Vacant(e) => {
                let s = Store::open_read_only(e.key()).ok().map(Arc::new)?;
                e.insert(Arc::clone(&s));
                s
            }
        };

        // Warning: len() does a full scan. See "Cost of Statistics" below.
        let triple_count = store.len().ok();
        let named_graph_count = Some(store.named_graphs().count() as u64);

        Some(ServiceStatistics { triple_count, named_graph_count })
    }
}
```

#### Step 1d: Wire through `SparqlEvaluator`

In `lib/oxigraph/src/sparql/mod.rs`, the `SparqlEvaluator::into_evaluator`
method passes the `LocalStoreServiceHandler` both as a `DefaultServiceHandler`
and as a `StatisticsProvider` when the handler is configured.

The `PreparedQuery::explain` path already calls `Optimizer::optimize_graph_pattern`.
It would instead call `optimizer.optimize_graph_pattern(pattern)` where the
optimizer holds the statistics reference.

### Impact analysis

Consider a federated query joining a 10M-triple "facts" store against a
100-triple "reference" store:

```sparql
SELECT ?entity ?label ?region WHERE {
  SERVICE <oxigraph:///data/facts> {
    ?entity a ex:Company ; rdfs:label ?label ; ex:region ?region
  }
  SERVICE <oxigraph:///data/reference> {
    ?region ex:isoCode "FR"
  }
}
```

**Without statistics** (current behavior):

Both SERVICE inner patterns have similar structure (one triple with bound
subject + predicate, two with bound subject + variable object), so the
heuristic estimates them identically at ~100 rows each.  The optimizer
picks arbitrarily; if it builds the hash table from the 10M-row facts
store and probes with 100 reference rows, it does 100 lookups — fine.
But if it builds from reference and probes with 10M rows, it does 10M
lookups into a tiny hash table — wasteful.

**With statistics:**

- `facts` store: `triple_count = 10_000_000`
- `reference` store: `triple_count = 100`

The optimizer seed-picks `reference` as the smallest first child, builds a
hash table from its 100 results, then probes with the facts result stream.
The difference: hash-table build is O(100) instead of O(10M), probe count is
unchanged, but memory pressure and initial startup are dramatically better.

**Quantitative estimate:** For a join of stores of size N and M with
shared join keys, the current planner may build a hash table of size max(N,M).
With statistics, the build side is always the smaller store, reducing peak
memory by up to N/M.  For a 10M vs. 100 store join, this is a 100,000×
reduction in hash-table memory.

### Cost of statistics collection

`Store::len()` performs a full RocksDB scan.  On a 10M-triple store, this
takes ~100–500ms (storage dependent).  This is acceptable once per query
plan but NOT per SERVICE invocation.

Mitigations (see Tier 3):
1. Cache statistics in the `LocalStoreServiceHandler` with a TTL.
2. Use RocksDB's `GetApproximateSizes()` for O(1) estimates.
3. Compute statistics at load time and persist them in a side key.

---

## Tier 2: Filter Push-Down into SERVICE (Low Effort, Moderate Impact)

### Current behavior

In `push_filters`, SERVICE patterns are opaque — filters wrap around them
rather than being pushed into the inner pattern:

```rust
GraphPattern::Service { .. } => {
    // TODO: we can be smart and push some filters
    // But we need to check the behavior of SILENT that can
    // transform no results into a singleton
    GraphPattern::filter(pattern, Expression::and_all(filters))
}
```

### Opportunity

When the SERVICE target is a local store, filters on variables that are
bound by the inner SERVICE pattern AND not shared with other join children
can be pushed into the SERVICE's inner pattern, reducing the result set
before it enters the main join pipeline.

### Implementation

In `push_filters`, for `Service` patterns, check which filters use only
variables bound by the SERVICE's inner pattern (via
`infer_graph_pattern_types`).  Push those filters into the inner pattern.

Constraint: only do this when `silent = false`.  When SILENT is true,
filters that eliminate all rows would change the semantics (SILENT
converts empty results to a singleton row).

```rust
GraphPattern::Service { name, inner, silent } => {
    let inner_types = infer_graph_pattern_types(&inner, input_types.clone());
    let mut pushable = Vec::new();
    let mut remainder = Vec::new();
    for filter in filters {
        if !*silent
            && !does_contain_exists(&filter)
            && are_all_expression_variables_bound(&filter, &inner_types)
        {
            pushable.push(filter);
        } else {
            remainder.push(filter);
        }
    }
    let inner = if pushable.is_empty() {
        *inner
    } else {
        GraphPattern::filter(*inner, Expression::and_all(pushable))
    };
    GraphPattern::filter(
        GraphPattern::service(inner, name, *silent),
        Expression::and_all(remainder),
    )
}
```

### Impact

For a query like:

```sparql
SELECT ?label WHERE {
  SERVICE <oxigraph:///data/facts> {
    ?entity a ex:Company ; rdfs:label ?label
  }
  FILTER(CONTAINS(?label, "Acme"))
}
```

Without push-down, all `?label` values are shipped from the SERVICE to the
outer filter.  With push-down, the filter runs inside the SERVICE — the
inner evaluator applies it during quad scan, reducing the result set early.

This is especially impactful when the filter is highly selective (e.g.,
`?label = "Acme Corp"` filters 2M labels down to 1).

---

## Tier 3: Lightweight Statistics (Low-Medium Effort, Enables Tier 1 at Scale)

### Problem

`Store::len()` is too expensive to call per-query for large stores.

### Solution A: Cached approximate statistics

Add a `CachedStatistics` wrapper in `local_service.rs`:

```rust
struct CachedStatistics {
    triple_count: AtomicU64,
    named_graph_count: AtomicU64,
    last_updated: AtomicI64, // Instant would require mutex
}

impl LocalStoreServiceHandler {
    fn get_statistics(&self, path: &Path) -> Option<ServiceStatistics> {
        // Check cache first
        if let Some(cached) = self.stats_cache.get(path) {
            if cached.is_fresh(Duration::from_secs(60)) {
                return Some(cached.to_service_statistics());
            }
        }
        // Recompute
        let store = self.get_or_open(path)?;
        let tc = store.len().ok()?;
        let ngc = store.named_graphs().count() as u64;
        let cached = CachedStatistics::new(tc, ngc);
        self.stats_cache.insert(path.to_path_buf(), cached);
        Some(ServiceStatistics {
            triple_count: Some(tc),
            named_graph_count: Some(ngc),
        })
    }
}
```

### Solution B: RocksDB approximate sizes (preferred)

RocksDB exposes `GetApproximateSizes()` which returns the on-disk byte
size of key ranges in O(1).  The `oxrocksdb-sys` crate already links
against this API.  We can wrap it:

```rust
// In storage/rocksdb.rs or a new module:
impl Storage {
    /// Return the approximate on-disk size of each SPO index.
    /// Multiply by an empirically-derived bytes-per-triple factor
    /// to estimate cardinality.
    pub fn approximate_sizes(&self) -> Vec<u64> { ... }
}
```

Then derive a cardinality estimate:

```rust
fn estimate_triple_count(approximate_bytes: u64) -> u64 {
    // Typical RocksDB entry for Oxigraph: ~40-80 bytes per triple
    approximate_bytes / 60
}
```

This gives a ±30% estimate in under a millisecond — perfectly adequate for
join ordering decisions.

### Solution C: Predicate frequency (future)

For more precise inner-pattern estimates, pre-compute per-predicate
cardinalities during bulk load and store them in a dedicated RocksDB
column family.  The optimizer could then estimate `?x rdf:type ex:Company`
as `pred_count[rdf:type] * selectivity_factor` instead of the generic 1,000.

This is a larger project (requires schema changes, migration) and is
deferred to a separate design document.

---

## Tier 4: Lateral Join for SERVICE (Medium-High Effort, Situational)

### Problem

When the optimizer encounters a join of pattern A with SERVICE B where
A binds variables used in B, it always uses a hash join:

```
HashJoin(build=A, probe=B)
```

This requires materializing ALL results of A into a hash table, then
probing with B.  If A produces 1M rows, this uses significant memory.

### Opportunity

With `sep-0006` (Lateral / ForLoopJoin), the optimizer can sometimes
rewrite a hash join into a lateral join:

```
ForLoopJoin(A, SERVICE B)
```

which evaluates B once per row from A.  This avoids materializing A
entirely.  Today, `Service` is explicitly excluded from
`is_fit_for_for_loop_join` (line 1085):

```rust
GraphPattern::Service { .. } => false,
```

### Implementation

Allow `Service` to participate in for-loop joins when:
1. The target is a local store (opening a snapshot per row is cheap — no
   HTTP overhead)
2. The SERVICE inner pattern is simple enough (a single `QuadPattern` or
   a small join of bound-subject patterns)
3. The outer cardinality is moderate (<10K rows, or statistics say the
   lateral cost is cheaper)

The `is_fit_for_for_loop_join` function gains a `Service` branch:

```rust
GraphPattern::Service { inner, .. } => {
    is_fit_for_for_loop_join(inner, global_input_types, entry_types)
}
```

And the cost model in `reorder_joins` uses `estimate_lateral_cost` when
the SERVICE qualifies, taking into account that each inner evaluation
requires a new `DatasetView` snapshot (O(1) for RocksDB).

### Impact

For a query like:

```sparql
SELECT ?entity ?label WHERE {
  VALUES ?entity { ex:A ex:B ex:C }       # 3 rows
  SERVICE <oxigraph:///data/facts> {
    ?entity rdfs:label ?label             # indexed lookup per row
  }
}
```

Without lateral join: build hash table from 3 VALUES rows, probe with
potentially millions of SERVICE results.
With lateral join: iterate 3 VALUES rows, for each do an indexed
`?entity rdfs:label ?label` lookup in the local store.

The lateral join is faster because it avoids scanning the entire SERVICE
result set — the bound variable `?entity` turns the SERVICE inner pattern
into an indexed point lookup.

**Quantitative estimate:** For N outer rows and a SERVICE store of size M,
hash join cost is O(M) (scan entire SERVICE result).  Lateral join cost
is O(N × log M) (indexed lookups).  When N ≪ M (the typical case),
lateral join wins by a factor of M/N.  For N=3 and M=10M, this is a
~3,000,000× improvement.

---

## Summary: Recommended Implementation Order

| Tier | Name | Effort | Impact | Prerequisites |
|------|------|--------|--------|---------------|
| 1 | Cardinality statistics | ~150 LOC | High for multi-SERVICE joins | None |
| 2 | Filter push-down | ~50 LOC | Moderate for selective filters | None |
| 3a | Cached statistics | ~80 LOC | Enables Tier 1 at scale | Tier 1 |
| 3b | RocksDB approximate sizes | ~100 LOC | Enables Tier 1 at scale | Tier 1 |
| 4 | Lateral SERVICE joins | ~120 LOC | Situational (small outer, large inner) | sep-0006, Tier 1 |

**Immediate recommendation:** Implement Tier 1 + Tier 2 together (~200
lines across 3 crates).  These are the highest-value, lowest-risk changes.
Tier 3b should follow to make Tier 1 fast enough for production use.
Tier 4 is worth doing but requires more careful testing around snapshot
lifecycle in tight loops.
