# Fused SERVICE Implementation — Exact Record of What Was Built (R1)

> **Implementing from scratch? Read `fused_service_implementation_guide.md` first.** This doc is the
> *record* of what shipped on `basenote-bench2` (a branch that already had streaming `ServiceContext` and
> statistics work). The guide walks the full implementation starting from a **clean oxigraph with only basic
> `oxigraph://` SERVICE support and no query optimizations** — no spareval or sparopt changes required.

## Status

- **Branch:** `basenote-bench2`
- **Date:** 2026-08-08
- **Predecessor docs:** `service_boundary_design.md` (design + R1 status), `service_cache_design.md`
  (refuted cache premise), `local_service_optimizations.md`, `local_store_optimizations.md`, `local_service.md`.
- **This doc** is the precise, code-level record of the fused-SERVICE implementation (design doc §7.3 / §8),
  including every file touched, every function added, the exact guard conditions, and measured results.
- All 121 `oxigraph` tests pass; the implementation is clippy-clean (only pre-existing warnings remain).

---

## 1. The Idea in One Paragraph

`EncodedTerm` is a **keyless 128-bit SipHash of the term string** (`StrHash`), so the *same string* produces the
*identical* `EncodedTerm` in every oxigraph store, independent of load order. That makes a service store's encoded
terms **valid terms of the outer dataset** — the entire `externalize → sparesults::QuerySolution → channel →
internalize` round-trip of the handler path is unnecessary for evaluation. The fused solution exploits this by
rewriting constant `SERVICE <oxigraph://path> { inner }` patterns into `GRAPH <oxigraph://path> { inner }` patterns
at bind time, and evaluating them against a **merged `QueryableDataset`** (`FusedDataset`) that routes the virtual
graph name to the service store's RocksDB snapshot. Terms flow by identity; strings are only materialized when
output or an expression actually needs them.

---

## 2. Files Changed (Complete List)

| File | What changed |
|---|---|
| `lib/oxigraph/src/sparql/dataset.rs` | **+265 lines**: new `FusedService` (pub(crate)) and `FusedDataset` (pub) types implementing `QueryableDataset` + `StrLookup` |
| `lib/oxigraph/src/sparql/local_service.rs` | **+444/−…**: `get_or_open_store` extracted from the handler; new `fuse_query`, `fuse_pattern`, `contains_graph_pattern`, `try_fuse_service`, `open_store_read_only` |
| `lib/oxigraph/src/sparql/mod.rs` | **+73**: `with_local_service_handler`, `local_service_handler` field on `SparqlEvaluator`/`PreparedSparqlQuery`, `bind_local_services`, `on_store`/`on_transaction` return-type change |
| `lib/oxigraph/tests/local_service.rs` | **+223**: 6 new fused-path tests |
| `lib/oxigraph/benches/local_service.rs` | **+35**: new `SERVICE fused` benchmark arm + `direct == handler == fused` correctness assertion |
| `lib/spareval/src/service.rs` | **+51**: `ServiceContext` struct (incoming_bindings, suggested_limit, cancellation_token); `DefaultServiceHandler::handle_with_context` (default method); `ErrorConversionServiceHandler::handle_with_context` |
| `lib/spareval/src/eval.rs` | **+15**: `evaluate_service` now builds a `ServiceContext` (with the dataset's cancellation token) and calls `handle_with_context` |
| `lib/spareval/src/lib.rs` | **+2**: re-export `ServiceContext` |

Note: the `ServiceContext`/`handle_with_context` plumbing in `spareval` predates the fused work (it belongs to the
streaming-handler iteration) but is listed because the fused path is built on top of it and the diff is part of the
current working tree.

---

## 3. Core Mechanism

### 3.1 Term identity across stores (`dataset.rs`, `StrHash`)

`lib/oxigraph/src/storage/numeric_encoder.rs`:

```rust
pub struct StrHash { hash: [u8; 16] }   // 128-bit SipHash-2-4 of the string, keyless
pub enum EncodedTerm { NamedNode { iri_id: StrHash }, ... }
```

`StrHash::new(value)` is a deterministic function of the string bytes. Consequently:

- **Joins, filters, `combine_with`, hash builds** compare encoded terms by value (memcmp of 16 bytes ≈ 2–5 ns).
- **`get_str`** (string materialization) can be routed to *any* store that owns the hash — the outer dictionary is
  tried first, then each fused service store; any owner is correct.
- **`insert_str`** avoids duplicating a string already present in the outer store or any fused store.

The one exception is **blank nodes**: their IDs are store-local, so the same string can encode to different hashes
in different stores (and different strings can collide). This is documented as a known limitation (§9) and is the
only non-portable encoding.

### 3.2 The rewrite: `SERVICE` → `GRAPH` (`local_service.rs`)

Entry point `fuse_query(query, handler, outer, services) -> Result<Query, StorageError>`:

- Handles all four query forms (`Select` / `Construct` / `Describe` / `Ask`) by rewriting `query.pattern` via
  `fuse_pattern` and preserving the `Query` wrapper.
- Returns the modified query. On error, the caller (`bind_local_services`) **discards the fused query and the
  collected services** and evaluates the original query (SERVICE nodes keep the handler path).

`fuse_pattern` recursively rewrites a `GraphPattern`:

- **`GraphPattern::Service { name, inner, silent }`** — the only interesting case:
  - `inner` is first rewritten recursively (fused SERVICEs can nest),
  - fusion happens only when **all** of these hold:
    1. `!contains_graph_pattern(&inner)` — the inner pattern must not contain `GRAPH` nodes
       (see §5, guard 1),
    2. `!silent` — `SERVICE SILENT` is never fused (guard 2),
    3. `name` is a **constant** `NamedNodePattern::NamedNode` (variable service names keep the handler path),
    4. `try_fuse_service(...)` returns `true`.
  - If fused: `GraphPattern::Graph { name, inner }` (the `silent` flag is dropped — fusion only happens when the
    store opened successfully at plan time, so the silent semantics are preserved by construction for openable
    stores).
  - Otherwise the node is kept as `GraphPattern::Service { name, inner, silent }`.
- Every other pattern variant is rewritten structurally (recurse into children), with `Bgp`, `Path`, `Values`
  returned unchanged.

### 3.3 The fused dataset (`dataset.rs`)

```rust
pub(crate) struct FusedService {
    graph_name: EncodedTerm,          // the encoded oxigraph:// SERVICE IRI
    view: DatasetView<'static>,       // snapshot of the service store (default graph = SERVICE's dataset)
}

pub struct FusedDataset<'a> {
    outer: DatasetView<'a>,
    services: Vec<FusedService>,
    extra: RefCell<HashMap<StrHash, OxString, BuildHasherDefault<StrHashHasher>>>,  // overlay dict
}
```

`QueryableDataset` implementation:

- **`internal_quads_for_pattern`** — the routing decision:
  - `graph_name == Some(Some(g))` and `g` matches a fused service → scan the service store's **default graph**
    (`Some(None)`), and **stamp** `quad.graph_name = Some(g.clone())` on every returned quad (see §4).
  - any other graph name → delegate to `outer.internal_quads_for_pattern` unchanged.
  - `None` / `Some(None)` (default graph / union of named graphs) → delegate to outer; service data is
    **not** part of the union-of-named-graphs, matching handler semantics.
- **`internal_named_graphs`** — intentionally delegates to outer only. Virtual service graphs are not enumerated:
  `GRAPH ?g` iterates only the outer dataset's named graphs (parity with the handler path, where the SERVICE
  dataset is not part of `GRAPH ?g`).
- **`contains_internal_graph_name`** — `true` for fused service graph names, else delegates to outer. This lets
  `GRAPH <oxigraph://...>` pattern evaluation proceed (spareval's `Graph` evaluator checks this before scanning).
- **`internalize_term`** — encodes via `(&term).into()` and inserts the string into the overlay only if neither
  the outer reader nor any fused service reader already contains it.
- **`externalize_term` / `externalize_expression_term` / `internalize_expression_term` /
  `internal_term_effective_boolean_value`** — mirrored from `DatasetView` (identical implementations; the
  expression-term arms handle the inline literal fast paths, `Triple` terms under `rdf-12`, and the
  `UnexpectedDefaultGraph` corruption guard).

`StrLookup` implementation (`get_str`): overlay → outer → each fused service (in order), returning the first hit.

---

## 4. Graph-Name Stamping (the `UnexpectedDefaultGraph` fix)

**Bug found during implementation:** the optimizer pushes a constant graph name down into the fused `GRAPH`'s
inner quad patterns, producing `QuadPattern`s with an explicit graph name. spareval's quad-pattern evaluator
(`eval.rs` ~805) requires every returned quad to carry `graph_name == Some(graph_term)` and raises
`UnexpectedDefaultGraph` when it is `None`. Since a fused service exposes the service store's *default graph*
(which scans return as `graph_name = None`), the first fused tests failed with `UnexpectedDefaultGraph`.

**Fix:** `FusedDataset::internal_quads_for_pattern` **stamps the virtual graph term onto every quad** returned
from a fused service:

```rust
Box::new(
    service
        .internal_quads_for_pattern(subject, predicate, object, Some(None))
        .map(move |quad| {
            let mut quad = quad?;
            quad.graph_name = Some(graph_name.clone());
            Ok(quad)
        }),
)
```

This is semantically sound: the stamped graph name is exactly the graph the evaluator asked for, and joins that
compare graph names (e.g. `GRAPH`-level joins) now work across fused and outer data.

---

## 5. Guard Conditions — When Fusion is Skipped (Handler Path Kept)

Fusion is a **semantics-preserving rewrite**; every case where it could change observable behavior falls back to
the streaming handler path:

1. **Inner pattern contains `GRAPH` nodes** (`contains_graph_pattern`). A fused SERVICE exposes the service
   store's *default graph* as a named graph of the merged dataset. A nested `GRAPH` inside the SERVICE addresses
   *named graphs of the service store*; after fusion the rewrite would route it to the *outer* dataset. Keeping
   the SERVICE node preserves handler semantics. (Nested `SERVICE` nodes inside a SERVICE are fine — they recurse.)
2. **`SERVICE SILENT`**. A mid-evaluation storage error must stay suppressed. The handler path already implements
   SILENT; fusion only proceeds when the store opens successfully at plan time, but a storage error during
   evaluation would otherwise propagate. Hence `silent` blocks fusion.
3. **Variable service name** (`SERVICE ?endpoint`). Only constant `NamedNodePattern::NamedNode` names are fused;
   the handler resolves variable names per binding.
4. **Non-`oxigraph://` IRI**. `try_fuse_service` strips the `oxigraph://` prefix; anything else returns `false`
   (HTTP or custom handlers keep their path).
5. **Store cannot be opened read-only**. `get_or_open_store` failure → `Ok(false)` → SERVICE node kept, so both
   the error case and the SILENT case keep their existing behavior (verified by tests).
6. **Outer dataset already has a named graph with the SERVICE IRI**. `outer.contains_internal_graph_name` is
   checked before registering the service; on collision the SERVICE keeps handler semantics and the outer named
   graph keeps its own data (verified by the collision test).
7. **`FROM NAMED` in the query dataset**. `bind_local_services` checks
   `query.dataset().is_some_and(|d| d.named.is_some())` and skips fusion entirely: the dataset specification
   would gate named-graph access and make the fused `GRAPH` return nothing, while the handler path is independent
   of the dataset specification.
8. **Any error during the rewrite** (`fuse_query` returns `Err`) → the caller clears collected services and runs
   the original query via the handler path.

---

## 6. API Surface

### `SparqlEvaluator`

```rust
pub fn with_local_service_handler(mut self, handler: LocalStoreServiceHandler) -> Self
```

- Same as `with_default_service_handler(handler)` (registers the handler for non-fused SERVICE calls) **plus**
  stores `handler.clone()` in `SparqlEvaluator.local_service_handler`, which is passed to `PreparedSparqlQuery`
  at parse/for-query time and drives fusion at bind time.
- Disables the default HTTP handler (like `with_default_service_handler`).
- Doc-commented with the fused-path caveats (blank nodes, eager store open).

### `PreparedSparqlQuery`

```rust
pub fn on_store(self, store: &Store) -> BoundPreparedSparqlQuery<'static, FusedDataset<'static>>
pub fn on_transaction<'b>(
    self,
    transaction: &'b Transaction<'_>,
) -> BoundPreparedSparqlQuery<'b, FusedDataset<'b>>
```

- Both now return `BoundPreparedSparqlQuery<..., FusedDataset<...>>` instead of
  `BoundPreparedSparqlQuery<..., DatasetView<...>>`. **Source-compatible for all chaining callers** (verified by
  search across `cli/`, `python/`, `js/`, `fuzz/`, `testsuite/`, benches, and tests — all call sites chain
  `on_store(...).execute()`).
- New private helper `bind_local_services(queryable_dataset)`:
  1. Snapshot the outer store (as before).
  2. Check `has_from_named`; if set, skip fusion (guard 7).
  3. If a `local_service_handler` is set, call `fuse_query(self.query.clone(), handler, &outer_view, &mut services)`.
     On `Ok`, replace `self.query`; on `Err`, clear `services` and keep the original query (guard 8).
  4. Wrap in `FusedDataset::new(outer_view, services)` and delegate to `on_queryable_dataset`.

### Visibility

- `FusedService` is `pub(crate)`; `FusedDataset` is `pub` (it appears in the public return type of
  `on_store`/`on_transaction`, same precedent as `DatasetView`), with private fields and `pub(crate)` methods —
  it is not meant to be constructed or used directly by consumers.
- `fuse_query` is `pub(crate)`; `fuse_pattern`, `contains_graph_pattern`, `try_fuse_service`,
  `open_store_read_only` are private.

---

## 7. Tests Added (`lib/oxigraph/tests/local_service.rs`, 6 new tests)

| Test | Verifies |
|---|---|
| `local_service_fused_matches_handler` | fused path returns exactly the handler path's results (2 labels) |
| `local_service_fused_federated_join` | two-store federated join works through the fused path |
| `local_service_fused_silent_on_missing_store` | `SERVICE SILENT` on a missing store yields one empty solution (handler fallback) |
| `local_service_fused_missing_store_is_error` | non-SILENT SERVICE on a missing store reports the error in the stream |
| `local_service_fused_outer_graph_collision` | outer named-graph IRI collision: GRAPH reads outer data, SERVICE reads the store, decoy never leaks into SERVICE |
| `local_service_fused_limit` | `LIMIT 1` produces exactly 1 solution |

---

## 8. Benchmark Changes and Measured Results

`lib/oxigraph/benches/local_service.rs`:

- `run_queries` gained a `fused: bool` parameter; a third arm `SERVICE fused` benchmarks
  `with_local_service_handler`.
- Correctness gate strengthened: `direct == handler == fused` for all 5 query pairs at both sizes, asserted and
  reported as `N=…: correctness verified OK (direct == handler == fused)`.
- Benchmark data is cached in `lib/oxigraph/benches/data/`; phases run N=1000 and N=5000 (criterion_main was
  TEMP-limited to those two phases; N=25000/100000 remain commented out).

Measured (5 query pairs, LIMIT 50, sample_size 10, machine-local, ±5 % variance):

| N | direct (named graphs) | SERVICE (handler) | **SERVICE fused** | fused vs handler | fused vs direct |
|---|---|---|---|---|---|
| 1000 | 46.4 ms | 92.8 ms | **6.7 ms** | ~14× faster | ~7× faster |
| 5000 | 2.3 ms | 390 ms | **1.3 ms** | ~310× faster | ~1.8× faster |

The fused path even beats the single-store named-graphs path at N=5000: joins run on identity terms with no
boundary cost, and each store's index is smaller (better cache locality). This closes the original
"~170–185× SERVICE gap" measured in `service_boundary_design.md` / `service_cache_design.md`.

---

## 9. Known Limitations (documented on `with_local_service_handler` and in the design doc §8)

1. **Blank nodes are the one non-portable encoding** and can collide across stores. Not handled in code; avoid
   fusing stores containing blank nodes for now.
2. **Fused SERVICE cardinality statistics are lost**: the rewrite bypasses `StatisticsProvider::service_statistics`
   (the optimizer only asks it for `Service` nodes). Join order on larger queries would benefit from a
   `graph_statistics`-style callback for virtual `GRAPH` names.
3. **The fallback handler path still spawns a thread + channel** per SERVICE evaluation (the streaming design).
   Tier 1 (synchronous streaming, design doc §4) would remove that for fallback cases.
4. **Eager store open at bind time**: local stores are opened read-only during `on_store`/`on_transaction` even
   if the SERVICE branch is never executed (a deliberate trade-off vs. the handler's lazy open).
5. **R2 (lateral + binding pushdown)** remains complementary and orthogonal; fused `GRAPH` nodes already pass
   `is_fit_for_for_loop_join`, so R2 applies to them directly once its cost gate is built.

---

## 10. Relationship to the Design Doc (`service_boundary_design.md`)

- This implements **R1** (§7.3, §8): the fused-dataset / virtual-GRAPH architecture — the recommended option
  that makes local SERVICE equivalent to named graphs.
- The implementation **did not** change `sparopt` (no optimizer edits): the SERVICE→GRAPH rewrite happens before
  planning, so `Graph` nodes inherit all existing join reordering, lateralization and statistics machinery.
- Tiers 1–3 of the design doc (§4) were **not** implemented: the measured R1 result (1.3 ms at N=5000) makes them
  unnecessary for the fused path; they remain fallback options for the handler path.
- The `service_cache_design.md` per-tuple cache was **not** implemented (its premise was refuted in
  `service_boundary_design.md` §2: SERVICE nodes are excluded from for-loop/lateral joins, so
  `evaluate_service` is called once per query execution, not per tuple).

---

## 11. Re-implementation on `basenote-bench3` (clean baseline, 2026-08-08)

The fused service was **re-implemented from scratch** on `basenote-bench3` (state of `0a78032f`: eager
`LocalStoreServiceHandler`, **no** `ServiceContext`/streaming/statistics) following the guide in
`fused_service_implementation_guide.md`. Differences vs the `basenote-bench2` record:

- **`lib/spareval` and `lib/sparopt` untouched** — confirms the guide's claim: fusion needs none of the
  later-branch machinery. 5 files changed (+950/−32): `sparql/dataset.rs`, `sparql/local_service.rs`,
  `sparql/mod.rs`, `tests/local_service.rs`, `benches/local_service.rs`.
- **9 fused-path tests** (vs 6): the original 6 plus 3 guard tests for nested-`GRAPH`-in-SERVICE,
  variable service names (behavior-parity with the handler path — oxigraph's evaluator surfaces
  `UnboundService` for VALUES-bound names on both paths), and `FROM NAMED` gating.
- **Benchmark** (N=1000 + N=5000, data cached):

  | N | direct (named graphs) | SERVICE (handler, eager) | **SERVICE fused** | fused vs handler | fused vs direct |
  |---|---|---|---|---|---|
  | 1000 | 38.5 ms | 82.6 ms | **6.5 ms** | ~13× faster | ~6× faster |
  | 5000 | 2.26 ms | 3.83 s | **1.24 ms** | ~3100× faster | ~1.8× faster |

  Correctness `direct == handler == fused` verified at both sizes. The N=5000 handler time is 3.83 s because
  this branch's fallback handler is the eager (non-streaming) baseline; the fused path bypasses it entirely.
- `criterion_main` TEMP-limited to `(n1000, n5000)` (n100000 data not cached).
- `on_store`/`on_transaction` now return `BoundPreparedSparqlQuery<'_, FusedDataset<'_>>`; the whole workspace
  (cli, python, js, testsuite, fuzz) compiles unchanged. Lib is clippy-clean; 118 oxigraph tests pass.
