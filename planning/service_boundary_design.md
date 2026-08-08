# Closing the SERVICE Gap: What the Last Benchmark Actually Revealed

## Status

- **Branch:** `basenote-bench2`
- **Predecessor docs:** `local_service_optimizations.md`, `service_cache_design.md`, `local_store_optimizations.md`
- **This doc supersedes** the diagnosis in `service_cache_design.md` (its cache premise is refuted below) and the
  "remaining bottleneck" note in `bench/results/basenote-bench2-streaming.json`.

---

## 1. Executive Summary

The local `SERVICE <oxigraph://...>` path is still ~170× slower than the equivalent named-graph query at
N=5000 (397 ms vs 2.3 ms per benchmark iteration). The previous session's hypothesis — *"a thread is created for
every record"* — is **wrong for the current implementation**, and the cache proposed in `service_cache_design.md`
made things *worse* because its core assumption (per-tuple `evaluate_service()` calls) is also wrong.

Empirical instrumentation (this run, machine-local, ~5% run-to-run variance) shows:

| Metric | N=1000 | N=5000 |
|---|---|---|
| direct (named graphs) | 40.4 ms | 2.31 ms |
| SERVICE (3 stores) | 91.3 ms | 396.7 ms |
| overhead ratio | 2.26× | **171.5×** |
| thread spawns per iteration | ~5 | ~12 |
| solutions crossing the boundary per iteration | ~19 K | ~86 K |
| per-solution boundary cost (SERVICE only) | ~4.8 µs | ~4.6 µs |

Thread spawns are ~2.5 per query (~12 per iteration). Solutions crossing the boundary are ~86 K per iteration.
That is **~1 thread per ~7,000 solutions** — the thread-per-call architecture is real, but it is **not** per-record.

**The real bottleneck is the per-solution dictionary round-trip at the SERVICE boundary.** Every SERVICE result
solution pays, per term:

1. **Produce side (~57% of boundary time):** RocksDB scan + `externalize_term` (a `get_str` point-read of the
   string dictionary per term) + `sparesults::QuerySolution` construction, then a `sync_channel` send.
2. **Consume side (~43%):** channel receive + `encode_bindings` → `internalize_term` into the *outer* dataset's
   dictionary + tuple construction + hash-join build/probe.

The named-graph (direct) path pays ~0 for all of this because terms stay internal `EncodedTerm`s of one
dictionary and never materialize. **The fix is to make the local SERVICE boundary as close to a
dictionary-identity map as possible: (Tier 1) delete the thread/channel, (Tier 2) add a per-query term-mapping
cache keyed by the service store's `StrHash` so repeated terms never materialize, (Tier 3) bypass the nested
evaluator for simple patterns.**

§7 evaluates the alternatives raised in review (Arrow, zero-copy, query restructuring) and reaches a stronger
conclusion: `EncodedTerm` is already a **portable, keyless hash of the string** (identical in every oxigraph store),
so a *fused-dataset* design (R1) can remove the boundary from the hot path entirely and land near the direct path
(~1–2× instead of 171×); a *lateral + binding-pushdown* plan shape (R2) removes the intermediate volume for
selective/LIMIT queries. Arrow is not applicable (see §7.1).

---

## 2. What the Instrumented Benchmark Shows

A temporary counter/timer was added to `LocalStoreServiceHandler::handle_with_context` (thread spawns, per-thread
solutions produced, cumulative produce/consume time), run, and then reverted (working tree is untouched).

Full run (both phases, current streaming code):

- 1,020 thread spawns, **4,984,330 solutions** sent through channels, 354 early exits (probe sides stopped by
  `LIMIT 50`).
- Per-thread solutions: min 51, median 4,930, max 15,002 → a large share of threads are fully drained
  **hash-join build sides**, not lazily-stopped probe sides.
- Produce (thread side) : consume (receiver side) ≈ **57 : 43** of boundary time.

Key structural fact (verified in `lib/spareval/src/eval.rs` + `lib/sparopt/src/optimizer.rs`):

- SERVICE nodes are excluded from for-loop/lateral joins (`is_fit_for_for_loop_join(Service) == false`), so every
  SERVICE child of a join is evaluated **exactly once per query execution** (the hash-join closure invokes
  `build(from)` / `probe(from)` once with the single top-level tuple).
- Therefore `evaluate_service()` is *not* called per record today, and a per-query cache keyed by service IRI
  (as proposed in `service_cache_design.md`) can **never hit** — it only adds eager `collect()` of every SERVICE
  result (defeating the lazy `LIMIT` benefit on probe sides), memory, and O(cache) filter scans. That is why
  "caching actually slowed down the process."

The "thread per record" concern becomes real only *after* binding pushdown is enabled (then the Service closure
runs per incoming tuple). Tier 1 removes the thread first, making pushdown safe to add later (Tier 4).

---

## 3. Root Cause: The Boundary Cost

For query Q2 `SERVICE(s0){?s rdf:type ?type} SERVICE(s1){?s rdfs:label ?label} ?s numeric LIMIT 50` the optimized
plan is `HashBuildLeftProbeRight(Join(HashBuildLeftProbeRight(s1, s0), numeric))`:

- inner join: build = s1 labels (~30 K) fully drained through the channel; probe = s0 types (~169 K) fully drained
  because its matched output feeds the **outer hash-join build** (~169 K tuples materialized even though only 50
  rows are ultimately returned).
- The direct named-graph version executes the *same* join shape, but its ~200 K tuples stay internal
  (`Vec<Option<EncodedTerm>>` + hash insert ≈ 2–5 ns/tuple) → 2.3 ms. The SERVICE version pays
  externalize → `QuerySolution` → channel → internalize per tuple → 397 ms.

Per-solution cost driver (measured ~4.6 µs at N=5000, ~2 terms/solution):

| Component | Est. share |
|---|---|
| produce: RocksDB iteration + `externalize_term` (`get_str` per term) | ~57 % |
| consume: recv + `internalize_term` (outer dict lookup/insert) + join | ~43 % |
| thread spawn + channel synchronization | small (<1 %) |

Because SERVICE results repeat terms heavily (169 K type solutions contain only ~5 K distinct subjects and ~34
distinct types), a per-query mapping cache can eliminate ~97 % of the term work.

---

## 4. Design

### Tier 1 — Synchronous streaming: delete the thread and the channel

**Why it works:** `Store::storage().snapshot()` returns `RocksDbStorageReader<'static>` (fully owned; see
`storage/rocksdb.rs:354`). The inner `QueryEvaluator` evaluation is **lazy** (`SimpleEvaluator` builds an iterator
pipeline; RocksDB scans happen on pull), and `EvalDataset` holds the dataset behind `Rc`, so the result iterator
owns its dataset. Evaluating synchronously therefore still streams lazily (the `LIMIT` early-termination that the
thread was added for is preserved) and can yield a `'static` iterator without any thread.

**Change** (`lib/oxigraph/src/sparql/local_service.rs`, ~40 LOC):

```rust
let reader = store.storage().snapshot();          // 'static, owned
let dataset = DatasetView::new(reader);           // DatasetView<'static>
let query = Query::Select(SelectQuery { pattern: query_pattern, base_iri, dataset: None });
let results = QueryEvaluator::new()
    .with_cancellation_token(cancel)
    .prepare(&query)
    .execute(dataset)?;                            // QueryResults<'static>: dataset kept alive via Rc
// Return the lazy iterator directly; no channel, no thread.
```

If the compiler refuses to infer `'static` (lifetime tied to the local `dataset` value), fall back to keeping the
`DatasetView` alive in the returned iterator via a small `KeepAlive` wrapper struct holding
`Rc<DatasetView<'static>>` next to the iterator — the reader is already `'static`, so this is always possible.
No trait change required if the iterator is `'static`; otherwise add a defaulted
`handle_blocking()`-style borrowed variant (the fallback in `service_cache_design.md`).

**Removes:** ~12 thread spawns/iteration, per-solution mutex + allocation churn of the channel, and the
`QuerySolution` ↔ channel copy; keeps full laziness.
**Expected:** 397 ms → ~220–280 ms at N=5000 (produce+consume drop by the channel/alloc share).

### Tier 2 — Per-query term-mapping cache (the core fix)

**Change the boundary from "materialized Terms" to "mapped internal terms".**

Add a defaulted trait method in `lib/spareval/src/service.rs` (HTTP handlers keep the old path via the default):

```rust
pub trait DefaultServiceHandler {
    // existing: handle() / handle_with_context() -> QuerySolutionIter<'static>

    /// Stream solutions whose terms are already mapped into the caller's internal
    /// representation via `encode`. `encode(service_term) -> Result<T>` may cache.
    fn handle_with_context_encoded<T>(
        &self,
        service_name: &NamedNode,
        pattern: &GraphPattern,
        base_iri: Option<&Iri<OxString>>,
        context: ServiceContext,
        encode: &mut dyn FnMut(oxrdf::Term) -> Result<T, QueryEvaluationError>,
    ) -> Result<Box<dyn Iterator<Item = Result<Vec<Option<T>>, QueryEvaluationError>> + '_>,
        QueryEvaluationError> { /* default: materialize Terms, call encode per term */ }
}
```

For `LocalStoreServiceHandler` the implementation runs the (now synchronous, Tier 1) inner evaluation and maps each
term through `encode`. The caller (`evaluate_service` in `lib/spareval/src/eval.rs`) supplies a closure backed by a
per-`evaluate_service`-call cache:

```rust
let mut term_cache = HashMap::<(u64, TermKind), D::InternalTerm>::new(); // key = service StrHash + kind
let encode = |term: Term| {
    let hash = str_hash(&term);            // StrHash is a stable hash of the term's string
    Ok(*term_cache.entry((hash, kind_of(&term))).or_insert_with(|| dataset.internalize_term(term)))
};
```

Key points:

- `StrHash` is a deterministic hash of the term string, identical in the service store and the outer store, so the
  **service side can look up the outer dictionary by hash without materializing the string** on cache hits
  (~97 % of occurrences in the benchmark: ~5 K distinct terms across 169 K solutions).
- On misses the string is materialized once (one `get_str` + one outer-dict insert), then cached.
- The cache is per SERVICE-node evaluation (per call), which is exactly the repetition window that matters.
- Cancellation: cache population happens while pulling the lazy iterator; a cancelled query simply stops pulling
  (same semantics as today).

**Expected:** per-solution boundary cost ~4.6 µs → ~0.5–1 µs → N=5000 ≈ 40–90 ms (~15–40× direct).

### Tier 3 — Direct index evaluation for simple SERVICE patterns (optional, cheap)

For `SERVICE { QuadPattern }` (the benchmark shape), skip the nested `QueryEvaluator` + `SimpleEvaluator`
machinery entirely in the handler: take `reader.quads_for_pattern(s, p, o, None)` (RocksDB index scan) and map
each quad's terms through the Tier-2 `encode` cache, producing `Vec<Option<T>>` rows directly.

- Removes per-call nested-evaluator setup (evaluator construction, `graph_pattern_evaluator`, `decode_bindings`
  pipeline).
- Only valid when the inner pattern is a single quad pattern with no filters/joins; otherwise fall back to
  Tier 1/2 path.
- **Expected:** another ~30–50 % on top of Tier 1+2.

### Tier 4 — Deferred: binding pushdown, LIMIT propagation, lateral SERVICE joins

From `local_store_optimizations.md` (Tier 4) — now *safe* only because Tier 1 removed the thread:

1. Populate `ServiceContext.incoming_bindings` in `evaluate_service` from the `from` tuple (the VALUES-injection
   code in `local_service.rs` is already written and currently dead). This makes `evaluate_service` per-tuple —
   which today would mean per-tuple threads, hence the ordering constraint.
2. `suggested_limit` propagation: only when the SERVICE is in a position where an inner `Slice` is semantically
   valid.
3. `is_fit_for_for_loop_join` for local `Service` nodes.

These help *selective* queries. Note: with `LIMIT 50` the dense benchmark's intermediate volume is **not** intrinsic
— it is an artifact of the hash-join plan shape. A lateral plan that point-looks-up the SERVICE per outer tuple
produces 50 rows from ~1–2 subjects (each has ~34 types × ~6 labels), i.e. a handful of indexed lookups instead of
~200 K tuple processing (§7.4). This is the benchmark's biggest single lever, and it is safe only after Tier 1.

---

## 5. Implementation Plan

| # | Change | File(s) | Effort | Risk |
|---|---|---|---|---|
| 1 | Synchronous streaming (no thread/channel) | `lib/oxigraph/src/sparql/local_service.rs` | ~40 LOC | Low |
| 2 | `handle_with_context_encoded` trait method + default | `lib/spareval/src/service.rs`, `lib/spareval/src/eval.rs` | ~80 LOC | Medium (trait surface) |
| 3 | `StrHash`-keyed term-mapping cache in `evaluate_service` | `lib/spareval/src/eval.rs` | ~50 LOC | Medium |
| 4 | Direct index evaluation for single-quad SERVICE patterns | `lib/oxigraph/src/sparql/local_service.rs` | ~60 LOC | Medium (correctness) |
| 5 | Re-run instrumented benchmark; verify correctness + perf | bench + `lib/oxigraph/tests/local_service.rs` | — | — |

Order: 1 → 2+3 (one commit) → 4 → 5. Tier 4 only as a follow-up branch.

### Validation

- `cargo test -p oxigraph --features rocksdb --test local_service` (existing correctness suite; 275+ lines).
- `cargo bench -p oxigraph --bench local_service` — restore `criterion_main!(n1000, n5000)` (currently TEMP-limited
  to `n1000, n5000`; earlier runs also trimmed it).
- Targets (N=5000, per iteration, vs 2.3 ms direct):

| Stage | SERVICE time | Overhead vs direct |
|---|---|---|
| Current (streaming) | 397 ms | 171× |
| After Tier 1 | ~220–280 ms | ~100–120× |
| After Tier 1+2 | ~40–90 ms | ~15–40× |
| After Tier 1+2+3 | ~20–50 ms | ~10–20× |

The residual gap after Tiers 1–3 is the irreducible per-tuple cost of mapping *distinct* terms across two
dictionaries plus the hash-join materialization that the direct path also pays — closing it fully would require
evaluating the service pattern with a fused (single-dictionary) dataset view, which is a much larger refactor and
not recommended now.

---

## 6. Risks & Notes

- **Trait change** (`handle_with_context_encoded`): additive with a default implementation; HTTP handlers and the
  JS/Python bindings are unaffected. Keep the default's behavior byte-identical to the old path.
- **`'static` inference** in Tier 1: verified `snapshot() -> RocksDbStorageReader<'static>`; if the compiler still
  ties the result to the stack-local `DatasetView`, use an `Rc<DatasetView>` keep-alive wrapper (the reader is
  owned, so this is sound).
- **Cache correctness:** `StrHash` collisions are handled by the fallback to full `Term` materialization +
  `internalize_term` (which verifies equality), exactly like `DatasetView::insert_str` does today. Cache is
  per-call and single-threaded (`RefCell`/`HashMap`), so no locking.
- **Laziness must be preserved:** Tier 1 relies on the inner evaluation being lazy; Tier 2/3 must keep returning
  lazy iterators so `LIMIT`/early-drop still stops probe-side work (measured: 354 early exits today).
- Re-run the benchmark a few times; the machine showed ±5 % variance (this run: direct N=1000 40.4 ms vs 53 ms in
  the recorded JSON, likely different CPU/load).

---

## 7. Alternatives: Arrow, zero-copy, and query restructuring

### 7.1 Arrow — not applicable

Arrow's value is zero-copy **columnar exchange between Arrow-native components** (IPC / Flight / query engines).
This bottleneck has no serialization boundary: the boundary is a Rust function call between two crates in one
process. Adopting Arrow would mean encoding `EncodedTerm` streams into record batches (still materializing the
string bytes Arrow arrays must hold) and decoding them on the other side — a conversion layer and a heavy
dependency added for zero benefit. It also does not address the actual cost drivers (per-occurrence `get_str`
string materialization + two-dictionary churn). Arrow could be relevant later at the *results* layer (bindings,
server) if zero-copy result delivery is wanted — out of scope for this hot path.

### 7.2 Zero-copy IS feasible: `EncodedTerm` is already portable

`lib/oxigraph/src/storage/numeric_encoder.rs`:

```rust
pub struct StrHash { hash: [u8; 16] }                       // 128-bit SipHash-2-4 of the string, keyless
pub enum EncodedTerm { NamedNode { iri_id: StrHash }, ... } // small literals inline; big literals by StrHash
```

`StrHash::new(value)` is a deterministic function of the string bytes with a fixed key — the **same string
produces the identical `EncodedTerm` in every oxigraph store**, independent of load order (blank-node IDs are the
only non-portable variant, and blank nodes are scoped per-store anyway).

Consequence: **a service-store `EncodedTerm` IS a valid outer-store `EncodedTerm`.** Hash-table joins, filters,
and `combine_with` compare them by value (memcmp of a 16-byte hash ≈ 2–5 ns) — the entire
`externalize → Term → internalize` round-trip is unnecessary for evaluation. Strings only need to materialize
when output or an expression actually needs them (`get_str`), once per distinct term, on demand.

This is the real "zero-copy": the currency is a u128 hash, not byte buffers. The benchmark's 169 K solutions
contain ~5 K distinct terms, and Q1–Q5 never touch service strings during evaluation (filters/joins are on
main-store numerics), so the boundary conversion is ~100 % waste.

### 7.3 R1 — Fused dataset: local SERVICE evaluated as a virtual GRAPH (preferred architecture)

Rewrite constant `oxigraph://` SERVICE nodes into `GRAPH <virtual:storeN> { inner }` over a merged
`QueryableDataset` that routes per-graph quad scans to the right store's `RocksDbStorageReader`. `EncodedTerm`s
pass through by identity (zero-copy); `get_str`/`contains_str` route to whichever reader owns the hash, with the
outer dictionary tried first. Service strings enter the outer overlay dict once per distinct term when output
requires them.

- Semantics: at plan time, open each constant local SERVICE store read-only; if it fails, keep the SERVICE node
  (handler path) so error/SILENT behavior is preserved. Variable service names and HTTP SERVICES keep the
  handler path unchanged.
- Bonus: SERVICE boundaries disappear from the optimizer, so joins reorder freely across local stores and the
  `StatisticsProvider` estimates apply to the virtual graphs.
- Expected: the benchmark's plan becomes the same 3-way hash join as the direct path, on identity terms
  → **~2–5 ms (≈1–2× direct)** for all queries, including full-result (no-LIMIT) ones.
- Cost: medium–high (a merged dataset adapter + SERVICE→GRAPH rewrite in `lib/oxigraph/src/sparql`); the
  `DatasetView` already implements the per-store half of it.

### 7.4 R2 — Lateral SERVICE joins + binding pushdown + LIMIT-awareness (plan shape; biggest single lever for the benchmark)

For selective/small-output queries, evaluate the SERVICE per outer tuple as an **indexed point lookup**: populate
`ServiceContext.incoming_bindings` from the `from` tuple (VALUES injection already written in
`local_service.rs`), so the inner query becomes `VALUES ?s {...} ?s rdf:type ?type` — a subject-bound RocksDB
prefix seek. With `LIMIT 50`, ~1–2 outer tuples suffice (each subject yields ~34 types × ~6 labels ≥ 50 rows),
so a query does a handful of point lookups instead of ~200 K tuple processing.

Prerequisites (all in this doc or `local_store_optimizations.md`):

1. Tier 1 (no thread) — per-tuple evaluation must not spawn threads.
2. Lean per-call setup — one snapshot reused across tuples; prepared evaluation reused; no per-call allocs.
3. Enable local SERVICES in `is_fit_for_for_loop_join` (e.g. `Service { inner, .. } => is_fit_for_for_loop_join(inner, ...)`) so the optimizer can place the SERVICE on the lateral right side.
4. Cost-model nudge: with pushdown the right side's estimate collapses, so greedy ordering must re-evaluate with
   `estimate_lateral_cost` (already present). Ordering is the delicate part: we want numeric-left / SERVICE-right.

Caveats: SILENT semantics, variable service names, OPTIONAL interactions, and queries that genuinely need full
SERVICE results (no LIMIT, aggregates) — those need R1/R3 instead. R2 and R1 are complementary: R2 removes
volume; R1 removes per-tuple cost. If R1 lands, R2's benchmark benefit shrinks (the fused hash join already costs
~2–5 ms), and R2 remains for extreme selectivity.

### 7.5 Relationship to Tiers 1–3 and recommendation

| Option | Mechanism | Benchmark (N=5000) | All queries | Effort | Risk |
|---|---|---|---|---|---|
| Tier 1 (sync, no thread/channel) | delete thread + channel | 397 → ~220–280 ms | yes | small | low |
| Tier 2/3 (StrHash cache + direct index eval) | incremental boundary cheapening | → ~20–90 ms | yes | medium | med |
| R2 (lateral + pushdown + LIMIT) | remove intermediate volume | → single-digit ms for LIMIT queries | selective only | medium | med |
| R1 (fused dataset / virtual GRAPH) | remove the boundary (portable hashes) | → ~2–5 ms | yes | med-high | med |

Recommended order: **Tier 1 first** (prerequisite for R2, simplifies R1/R3, independent win). Then **R1** as the
architecture that makes local SERVICE equivalent to named graphs — it is the honest answer to "make it like a
single file". Follow with R2 for highly selective workloads. Tier 2/3 remain the fallback if the R1 refactor is
judged too large for now; it also helps the HTTP service path's local-adjacent costs.

### 7.6 R1 vs R2 — head-to-head

**Mechanism (where the win comes from)**

- **R1 removes the boundary.** SERVICE results enter the join as identity `EncodedTerm`s, so per-tuple cost drops
  to the direct path's ~2–5 ns. All queries benefit, including full-result ones (no LIMIT, aggregates).
- **R2 removes the volume.** The plan evaluates SERVICE per outer tuple as an indexed point lookup, so ~50-row
  LIMIT queries do a handful of lookups instead of ~200 K tuple processing. Only selective/small-output queries
  benefit; queries that genuinely need full SERVICE results do not.

**Benchmark (N=5000, 5 queries, LIMIT 50)**

| | R1 | R2 |
|---|---|---|
| expected | ~2–5 ms (≈1–2× direct) | ~2–4 ms with current per-row boundary cost; sub-ms once combined with Tier 2 |
| coverage | all queries | LIMIT/selective only |

On this benchmark the two land roughly level (both ≈ direct or better); they are **complementary, not
substitutes** — R1 makes every tuple cheap, R2 makes the plan stop early.

**Optimizer interaction (the key difference in risk)**

- R1 is invisible to the optimizer: SERVICE→GRAPH rewrite happens before planning; `Graph` nodes already
  participate in join reordering, `is_fit_for_for_loop_join` and lateralization. The only optimizer touch is
  statistics for virtual graphs (extend the existing `StatisticsProvider` hook to virtual graph IRIs).
- R2 requires enabling `Service` in `is_fit_for_for_loop_join` (`optimizer.rs:1280` currently `false`), and here
  is the hazard: `reorder_joins` lateralizes **unconditionally when fit** (`optimizer.rs:864-879` — the plain-Join
  step has *no cost gate*, unlike the LeftJoin path at `:956` which is gated by `lateral_cost <= join_cost*100`).
  Turning on the flag would make **every** SERVICE join a lateral join — great for LIMIT queries, a ~10–30×
  regression for full-result joins (30 K indexed lookups vs a 200 K-op hash join). R2 therefore needs a
  cost-based gate for SERVICE lateralization (only when the left side is small or a small LIMIT is in scope).
  This gate is a generally useful optimizer improvement regardless.

**Effort / blast radius**

| | R1 | R2 |
|---|---|---|
| crates touched | oxigraph only (sparql layer + a merged `QueryableDataset` adapter) | spareval (incoming_bindings) + sparopt (fit + gate) + oxigraph (lean per-tuple path) |
| new public API | none (internal rewrite) | trait/cost-model changes |
| correctness surface | SILENT/error fallback for unopenable stores; blank-node scoping; `get_str` routing across readers | per-tuple VALUES semantics (shared vars only), OPTIONAL/EXISTS interactions, bnode bindings dropped from VALUES |

**Failure modes**

- R1: a merged dataset that misroutes `get_str`/`contains_str` (mitigate: try outer dict first, then each reader;
  any owner is correct since hashes are equal); bnode IDs are store-local but only observable at output, where the
  owning reader supplies the string.
- R2: aggressive lateralization regressing full-result queries (mitigate: the cost gate above); huge VALUES tables
  when the outer side is large (mitigate: push down only when the incoming set is small); `SILENT`/variable-name
  cases must keep the handler path.

**Decision rule**

- Goal is "SERVICE ≈ single file" (this benchmark): **R1** — it is the principled fix and makes R2's machinery
  applicable afterwards (fused GRAPH nodes already pass `is_fit_for_for_loop_join`).
- Goal is a general query-optimizer improvement (lateral vs hash across all joins): build R2's **cost-gated
  lateral** as a standalone optimizer feature; it then applies to R1's fused graphs for free.
- Both must sit on Tier 1: R2 hard-requires it (per-tuple evaluation must not spawn threads); R1 simply makes the
  handler moot for local services (no thread exists in the fused path).

---

## 8. R1 implementation status (2026-08-08)

> **Implementation guide:** see `fused_service_implementation_guide.md` for a step-by-step, code-level walkthrough
> of building this solution from a clean oxigraph baseline (basic `oxigraph://` SERVICE support, no query
> optimizations). `fused_service_implementation.md` is the exact record of what shipped on `basenote-bench2`.

R1 is **implemented** on the `basenote-bench2` branch and measured against the real benchmark.

### What was built

- `FusedDataset`/`FusedService` (`lib/oxigraph/src/sparql/dataset.rs`): a `QueryableDataset` that routes the
  virtual `GRAPH <oxigraph://...>` name to the service store's snapshot (its default graph) and stamps the graph
  name on every returned quad. Terms flow by identity: `StrHash` is a keyless SipHash of the string, identical in
  every store, so no externalization/internalization round-trip happens on the hot path. `get_str` falls back
  outer → services, and `insert_str` avoids duplicating strings already present in any store.
- `fuse_query`/`fuse_pattern`/`try_fuse_service` (`lib/oxigraph/src/sparql/local_service.rs`): rewrites
  `SERVICE <oxigraph://p> { inner }` → `GRAPH <oxigraph://p> { inner }` at bind time, opening the store read-only
  (cached in the handler) and registering the snapshot. The rewrite is **guarded**: it is skipped when the store
  can't be opened, when the outer dataset already has a named graph with that IRI, for `SERVICE SILENT`, for
  SERVICE inner patterns containing `GRAPH` nodes (they address the service store's named graphs), and for queries
  with `FROM NAMED` clauses (the dataset spec would gate the fused graph). All these fall back to the handler path.
- `SparqlEvaluator::with_local_service_handler` + `PreparedSparqlQuery` plumbing (`lib/oxigraph/src/sparql/mod.rs`).
  `on_store`/`on_transaction` now return `BoundPreparedSparqlQuery<'_, FusedDataset<'_>>` (source-compatible for
  chaining callers; `FusedDataset` is `pub` but `#[doc(hidden)]`-style internal).

### Measured results (bench/bsbm, 5 query pairs, LIMIT 50, sample_size 10)

| N | direct (named graphs) | SERVICE (handler path) | SERVICE fused | fused vs handler | fused vs direct |
|---|---|---|---|---|---|
| 1000 | 46.4 ms | 92.8 ms | **6.7 ms** | ~14× faster | ~7× faster |
| 5000 | 2.3 ms | 390 ms | **1.3 ms** | ~310× faster | ~1.8× faster |

Correctness: `direct == handler == fused` verified for all 5 query pairs at both sizes (the benchmark asserts it;
`lib/oxigraph/tests/local_service.rs` adds 6 fused-path tests incl. equivalence, SILENT fallback, missing-store
error, outer-graph collision, and LIMIT). The fused path even beats the single-file named-graphs approach because
join logic runs on identity terms and each store's index is smaller (better cache locality).

### Known limitations / follow-ups

- **Blank nodes are the one non-portable encoding** and can collide across stores. Not handled in code; fused
  stores containing blank nodes should be avoided for now (documented on `with_local_service_handler`).
- **Fused SERVICE cardinality statistics are lost**: the rewrite bypasses `StatisticsProvider::service_statistics`
  (the optimizer only asks it for Service nodes). Join order on larger queries would benefit from extending the
  stats hook to virtual graph names (a `graph_statistics`-style callback for `GRAPH` nodes).
- **Tier 1 (sync streaming fallback)**: the handler path used as fallback still spawns a thread + channel per
  SERVICE evaluation; Tier 1 would remove that for the fallback cases.
- **R2 (lateral + pushdown)** remains complementary and orthogonal; the fused GRAPH nodes already pass
  `is_fit_for_for_loop_join`, so R2 applies to them directly once the cost gate is built.
- **Eager store open at bind time** (even for never-executed SERVICE branches) — a deliberate trade-off vs. the
  handler's lazy open, documented on `with_local_service_handler`.
