# Fused SERVICE — From-Scratch Implementation Guide

*How to implement the fused-SERVICE solution (design doc R1) starting from a **clean oxigraph** that has
**only basic `oxigraph://` SERVICE support** and **no query optimizations**.*

## Status

- **Target branch:** any branch containing only the *basic* Local Service API.
- **Exact baseline assumed by this guide:** the state of commit `0a78032f` ("Implemented Local Service API"):
  - `LocalStoreServiceHandler` in `lib/oxigraph/src/sparql/local_service.rs` that **eagerly** evaluates the
    SERVICE pattern against a snapshot of the local store (no thread, no channel, no `ServiceContext`).
  - `lib/spareval` untouched: `DefaultServiceHandler::handle()` only (no `handle_with_context`), and
    `evaluate_service` calls `self.service_handler.handle(...)` directly.
  - `lib/oxigraph/src/sparql/mod.rs`: `on_store` returns `BoundPreparedSparqlQuery<'static>` (default type
    parameter `DatasetView`), no `local_service_handler` field, no `with_local_service_handler`.
  - No `StatisticsProvider` impl on the handler; no custom `sparopt` features.
- **Verified against:** the real implementation on `basenote-bench2` (all 121 oxigraph tests pass, clippy-clean).
  Every code block below is the exact code from that implementation, adapted only where the baseline differs
  (e.g. Step 1 shows the baseline's inline store-open logic being extracted).
- **Companion docs:** `service_boundary_design.md` (the design + why), `fused_service_implementation.md`
  (the record of what shipped on `basenote-bench2`).

---

## 0. Scope: What This Guide Covers (and what it deliberately does NOT)

**You will implement, end to end:**

1. A small refactor of `LocalStoreServiceHandler` (extract `get_or_open_store`).
2. `FusedService` + `FusedDataset` — a merged `QueryableDataset` in `lib/oxigraph/src/sparql/dataset.rs`.
3. `fuse_query` / `fuse_pattern` / `contains_graph_pattern` / `try_fuse_service` — the `SERVICE → GRAPH`
   rewrite in `lib/oxigraph/src/sparql/local_service.rs`.
4. `SparqlEvaluator::with_local_service_handler` + `PreparedSparqlQuery::bind_local_services` + the
   `on_store`/`on_transaction` return-type change in `lib/oxigraph/src/sparql/mod.rs`.
5. Six tests + a benchmark arm, then clippy/docs hardening.

**You will NOT touch:**

- `lib/spareval` — no `ServiceContext`, no `handle_with_context`, no `evaluate_service` change. The fused
  rewrite happens *before* the evaluator ever sees a `Service` node, so spareval stays byte-identical.
- `lib/sparopt` — the rewrite produces ordinary `Graph` nodes; the stock optimizer already handles `Graph`
  patterns (join reordering, filter push-down, lateral fit). No statistics hook is needed for correctness.
- The handler's internal execution strategy — the eager baseline `handle()` is a perfectly good **fallback**
  for the (few) cases fusion skips. The thread/channel streaming added later on `basenote-bench2` is an
  orthogonal optimization for that fallback, not part of fusion.

**Effort:** ~600 lines across 4 files + tests. Roughly one focused session per step.

---

## 1. The Baseline: What You Start From

### 1.1 Files and their role at the baseline

| File | Role at baseline | What you will add |
|---|---|---|
| `lib/oxigraph/src/sparql/dataset.rs` | `DatasetView<'a>`: a `QueryableDataset` over a `StorageReader<'a>` + an overlay string dict (`extra`) | `FusedService`, `FusedDataset` |
| `lib/oxigraph/src/sparql/local_service.rs` | `LocalStoreServiceHandler` (eager eval), `open_store_read_only` | `get_or_open_store`, `fuse_query`, `fuse_pattern`, `contains_graph_pattern`, `try_fuse_service` |
| `lib/oxigraph/src/sparql/mod.rs` | `SparqlEvaluator`, `PreparedSparqlQuery`, `BoundPreparedSparqlQuery<'a, D = DatasetView<'a>>`, `on_store`/`on_transaction` | `with_local_service_handler`, `local_service_handler` fields, `bind_local_services`, return-type change |
| `lib/oxigraph/tests/local_service.rs` | Basic handler tests | 6 fused-path tests |
| `lib/oxigraph/benches/local_service.rs` | `direct (named graphs)` + `SERVICE (3 stores)` arms | `SERVICE fused` arm + 3-way correctness assertion |

### 1.2 The baseline handler (the fallback contract, in full)

```rust
// lib/oxigraph/src/sparql/local_service.rs — baseline (abridged to the essentials)
use crate::model::{NamedNode, OxString};
use crate::sparql::dataset::DatasetView;
use crate::store::Store;
use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use oxiri::Iri;
use spareval::{DefaultServiceHandler, QueryEvaluationError, QueryEvaluator, QueryResults, QuerySolutionIter};
use spargebra::algebra::GraphPattern;
use spargebra::query::{Query, SelectQuery};
use std::path::{Path, PathBuf};
use std::sync::Arc;

const OXIGRAPH_SCHEME: &str = "oxigraph://";

#[derive(Clone)]
pub struct LocalStoreServiceHandler {
    stores: Arc<DashMap<PathBuf, Arc<Store>>>,
}

impl DefaultServiceHandler for LocalStoreServiceHandler {
    type Error = QueryEvaluationError;

    fn handle(
        &self,
        service_name: &NamedNode,
        pattern: &GraphPattern,
        base_iri: Option<&Iri<OxString>>,
    ) -> Result<QuerySolutionIter<'static>, Self::Error> {
        let path_str = service_name.as_str()
            .strip_prefix(OXIGRAPH_SCHEME)
            .ok_or_else(|| QueryEvaluationError::UnsupportedService(service_name.clone()))?;
        let path = PathBuf::from(path_str);

        // Open (or reuse) the store read-only.
        let store = {
            let path_for_open = path.clone();
            match self.stores.entry(path) {
                Entry::Occupied(entry) => Arc::clone(entry.get()),
                Entry::Vacant(entry) => {
                    let store = open_store_read_only(&path_for_open)
                        .map(Arc::new)
                        .map_err(|e| QueryEvaluationError::Service(Box::new(e)))?;
                    entry.insert(Arc::clone(&store));
                    store
                }
            }
        };

        // Snapshot + evaluate the inner pattern against the store's dataset.
        let reader = store.storage().snapshot();          // StorageReader<'static>
        let dataset = DatasetView::new(reader);
        let query = Query::Select(SelectQuery {
            dataset: None,
            pattern: pattern.clone(),
            base_iri: base_iri.cloned(),
        });
        let results = QueryEvaluator::new().prepare(&query).execute(dataset)?;
        match results {
            QueryResults::Solutions(iter) => {
                let variables: Arc<[_]> = iter.variables().into();
                let solutions: Vec<_> = iter.collect();   // eager collect → 'static
                Ok(QuerySolutionIter::new(variables, solutions))
            }
            _ => Err(QueryEvaluationError::Service(
                "SERVICE pattern evaluation returned non-solution results".into(),
            )),
        }
    }
}

fn open_store_read_only(_path: &Path) -> Result<Store, crate::store::StorageError> {
    #[cfg(all(not(target_family = "wasm"), feature = "rocksdb"))]
    { Store::open_read_only(_path) }
    #[cfg(not(all(not(target_family = "wasm"), feature = "rocksdb")))]
    { Err(crate::store::StorageError::from(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "Opening on-disk stores requires the 'rocksdb' feature",
    ))) }
}
```

Key facts you rely on:

- `Store::storage() -> &Storage` is `pub(super)` (visible from `crate::sparql`).
- `Storage::snapshot() -> StorageReader<'static>` — the snapshot is fully **owned** (RocksDB snapshots hold
  their own handle; memory snapshots hold an `Arc<Storage>` clone + version counter).
- `DatasetView` is the existing `QueryableDataset` over one reader; you will *reuse* it unchanged as the
  "outer" half of the fused dataset and as the per-service `view`.

### 1.3 The APIs you build on (where to look, exact signatures)

| Type / fn | Location | Signature you need |
|---|---|---|
| `StrHash` | `storage/numeric_encoder.rs:14` | `pub struct StrHash { hash: [u8; 16] }`; `StrHash::new(&str) -> Self` (keyless SipHash-2-4) |
| `EncodedTerm` | `storage/numeric_encoder.rs:47` | `enum EncodedTerm { DefaultGraph, NamedNode{iri_id: StrHash}, NumericalBlankNode{..}, SmallBlankNode(..), BigBlankNode{id_id}, SmallStringLiteral(..), BigStringLiteral{value_id}, ... }` — **not `Copy`** |
| `StrHashHasher` | `storage/numeric_encoder.rs:1186` | hasher for `HashMap<StrHash, _, BuildHasherDefault<StrHashHasher>>` |
| `insert_term` | `storage/numeric_encoder.rs:740` | `pub fn insert_term<F: FnMut(&StrHash, OxString)>(term: Term, encoded: &EncodedTerm, insert_str: &mut F)` |
| `Decoder` trait | `storage/numeric_encoder.rs:941` | `fn decode_term(&self, encoded: &EncodedTerm) -> Result<Term, StorageError>` |
| `StorageReader<'a>` | `storage/mod.rs:188` | `snapshot()`, `quads_for_pattern(subject, predicate, object, graph_name: Option<&EncodedTerm>) -> DecodingQuadIterator<'a>`, `named_graphs()`, `contains_named_graph(&EncodedTerm) -> Result<bool, _>`, `len()`, `is_empty()`, plus `StrLookup::get_str` |
| `QueryableDataset<'a>` | `spareval/src/dataset.rs:22` | full trait — see below |
| `InternalQuad<T>` | `spareval/src/dataset.rs:113` | `{ subject: T, predicate: T, object: T, graph_name: Option<T> }` |
| `DatasetView<'a>` | `sparql/dataset.rs` | `new(reader)`, `insert_str`, `QueryableDataset` impl, `StrLookup` impl |
| `GraphPattern` | `spargebra/src/algebra.rs` | variants: `Bgp, Path, Join, LeftJoin, Lateral, Filter, Union, Graph, Extend, Minus, Values, OrderBy, Project, Distinct, Reduced, Slice, Group, Service{name, inner, silent}` |
| `NamedNodePattern` | `spargebra/src/term.rs:349` | `enum NamedNodePattern { NamedNode(NamedNode), Variable(Variable) }` |
| `Query` / `SelectQuery` | `spargebra/src/query.rs` | `Query::{Select, Construct, Describe, Ask}`; `SelectQuery { dataset, pattern, base_iri }` |
| `BoundPreparedSparqlQuery<'a, D>` | `sparql/mod.rs:627` | `pub struct BoundPreparedSparqlQuery<'a, D: QueryableDataset<'a> = DatasetView<'a>>` |

The `QueryableDataset` trait (exact, from `spareval/src/dataset.rs:22`):

```rust
pub trait QueryableDataset<'a>: Sized + 'a {
    type InternalTerm: Clone + Eq + Hash + 'a;
    type Error: Error + Send + Sync + 'static;

    /// `graph_name`: `Some(None)` = default graph, `Some(Some(_))` = named graph,
    /// `None` = any named graph (NOT the default graph).
    fn internal_quads_for_pattern(
        &self,
        subject: Option<&Self::InternalTerm>,
        predicate: Option<&Self::InternalTerm>,
        object: Option<&Self::InternalTerm>,
        graph_name: Option<Option<&Self::InternalTerm>>,
    ) -> impl Iterator<Item = Result<InternalQuad<Self::InternalTerm>, Self::Error>> + use<'a, Self>;

    fn internal_named_graphs(&self) -> impl Iterator<Item = Result<Self::InternalTerm, Self::Error>> + use<'a, Self>;
    fn contains_internal_graph_name(&self, graph_name: &Self::InternalTerm) -> Result<bool, Self::Error>;
    fn internalize_term(&self, term: Term) -> Result<Self::InternalTerm, Self::Error>;
    fn externalize_term(&self, term: Self::InternalTerm) -> Result<Term, Self::Error>;

    // Optional overrides (externalize_expression_term, internalize_expression_term,
    // internal_term_effective_boolean_value) — you copy DatasetView's implementations.
}
```

### 1.4 The one insight everything rests on

`EncodedTerm` is a **portable, keyless 128-bit SipHash of the term string** (`StrHash::new` is a pure function
of the bytes, same key in every store). Therefore:

- The same string → the identical `EncodedTerm` in the outer store and in every service store.
- A service-store term is *already* a valid outer-store term: hash-join builds/probes, filters and
  `combine_with` compare 16-byte hashes by value (≈2–5 ns). **No externalize → `QuerySolution` → channel →
  internalize round-trip is needed for evaluation.**
- Strings materialize only when output/expressions demand them (`get_str`), and *any* owner reader can supply
  the string (they agree on the hash). The outer dictionary is tried first, then each service.
- **Blank nodes are the one non-portable encoding** (store-local IDs). The fused path does not handle
  cross-store blank-node identity — document this caveat (§7.3).

The boundary the handler pays per solution (~4.6 µs measured: RocksDB scan + `get_str` per term +
`QuerySolution` construction + channel + re-internalize) simply disappears for fused nodes.

---

## 2. Architecture of the Solution (5 bullets)

1. **At bind time** (`PreparedSparqlQuery::on_store` / `on_transaction`), open every constant
   `SERVICE <oxigraph://...>` store read-only (cached in the handler) and register its snapshot in a
   `FusedService` list.
2. **Rewrite the query algebra** `SERVICE <oxigraph://p> { inner }` → `GRAPH <oxigraph://p> { inner }` with
   `fuse_query`. Guards (§4.3) keep the `Service` node whenever rewriting could change semantics; those nodes
   keep the handler path.
3. **Evaluate against `FusedDataset`**: a `QueryableDataset` whose `outer` is the query's `DatasetView` and
   whose `services` map each virtual graph name to a service snapshot. `internal_quads_for_pattern` routes
   `GRAPH <oxigraph://p>` scans to service *p*'s default graph and **stamps** the graph name onto each quad.
4. **Terms flow by identity** through joins; `get_str` falls back outer → services; `insert_str` dedupes
   strings already present in any store.
5. **The optimizer never sees a SERVICE**: the rewrite happens before `QueryEvaluator::prepare`, so fused
   nodes are plain `Graph` nodes — standard join reordering, `is_fit_for_for_loop_join`, lateralization and
   filter push-down apply with zero `sparopt` changes.

---

## 3. Implementation Steps

> **Order matters.** Each step compiles and is independently verifiable. Steps 1–4 are the implementation;
> 5–7 are validation. You can commit after each.

---

### Step 1 — Extract `get_or_open_store` (`local_service.rs`)

**Goal:** make the store cache reusable by the fusion rewrite (which must open stores at bind time, not only
inside `handle`).

**Change:** replace the inline `Entry` logic in `handle()` with a method:

```rust
impl LocalStoreServiceHandler {
    /// Returns the store cached for the given path, opening it read-only and caching it if needed.
    pub(crate) fn get_or_open_store(&self, path: &Path) -> Result<Arc<Store>, QueryEvaluationError> {
        match self.stores.entry(path.to_path_buf()) {
            Entry::Occupied(entry) => Ok(Arc::clone(entry.get())),
            Entry::Vacant(entry) => {
                let store = open_store_read_only(entry.key())
                    .map(Arc::new)
                    .map_err(|e| QueryEvaluationError::Service(Box::new(e)))?;
                entry.insert(Arc::clone(&store));
                Ok(store)
            }
        }
    }
}
```

and in `handle()`:

```rust
let store = self.get_or_open_store(&path)?;
```

`open_store_read_only` already exists at the baseline — leave it.

**Verify:** `cargo check -p oxigraph --all-features`.

---

### Step 2 — `FusedService` + `FusedDataset` (`sparql/dataset.rs`)

**Goal:** the merged dataset. This is the heart of the solution.

**Imports to add** (baseline `dataset.rs` already imports the rest):

```rust
// Nothing new: DatasetView's imports already cover StrHash, StrHashHasher, EncodedTerm,
// insert_term, Decoder, StorageError, StorageReader, QueryableDataset, InternalQuad, ExpressionTerm.
```

**2a. The structs:**

```rust
/// A snapshot of a local `SERVICE <oxigraph://...>` store, exposed as a virtual named graph
/// of a [`FusedDataset`].
///
/// Service snapshots are `'static` (RocksDB snapshots are owned), and because
/// [`EncodedTerm`]s are keyless hashes of the term strings, the terms stored in a service
/// store are identical to the ones of any other oxigraph store: they flow through the
/// fused dataset by identity, with no externalization/internalization round-trip.
pub(crate) struct FusedService {
    /// The encoded virtual graph name (the `oxigraph://` SERVICE IRI).
    graph_name: EncodedTerm,
    /// The snapshot of the service store. Its default graph is the SERVICE's dataset.
    view: DatasetView<'static>,
}

impl FusedService {
    pub(crate) fn new(graph_name: EncodedTerm, view: DatasetView<'static>) -> Self {
        Self { graph_name, view }
    }

    pub(crate) fn graph_name(&self) -> &EncodedTerm {
        &self.graph_name
    }
}

/// Internal implementation detail of [`crate::sparql::PreparedSparqlQuery::on_store`] and
/// [`crate::sparql::PreparedSparqlQuery::on_transaction`]: the dataset used to evaluate a query
/// whose local `SERVICE <oxigraph://...>` patterns have been fused into virtual graphs.
///
/// `SERVICE <oxigraph://path> { pattern }` patterns are rewritten into
/// `GRAPH <oxigraph://path> { pattern }` by [`crate::sparql::local_service::fuse_query`];
/// evaluating such a `GRAPH` pattern against this dataset routes the quad scans to the
/// service store's snapshot, and terms pass through by identity (same `EncodedTerm` hash
/// in every store). Everything else is delegated to the outer [`DatasetView`].
///
/// This type is `pub` because it appears in the public return type of `on_store`/`on_transaction`
/// (like [`DatasetView`]), but it is not meant to be used directly: its fields are private and
/// all its methods are `pub(crate)`.
pub struct FusedDataset<'a> {
    outer: DatasetView<'a>,
    services: Vec<FusedService>,
    extra: RefCell<HashMap<StrHash, OxString, BuildHasherDefault<StrHashHasher>>>,
}

impl<'a> FusedDataset<'a> {
    pub(crate) fn new(outer: DatasetView<'a>, services: Vec<FusedService>) -> Self {
        Self {
            outer,
            services,
            extra: RefCell::new(HashMap::default()),
        }
    }

    fn find_service(&self, graph_name: &EncodedTerm) -> Option<&DatasetView<'static>> {
        self.services
            .iter()
            .find(|service| &service.graph_name == graph_name)
            .map(|service| &service.view)
    }

    /// Like [`DatasetView::insert_str`] but also checks the fused service stores: a string
    /// already available in any of them does not need to be duplicated in the overlay.
    fn insert_str(&self, key: &StrHash, value: OxString) {
        if let Entry::Vacant(e) = self.extra.borrow_mut().entry(*key) {
            if !matches!(self.outer.reader.contains_str(key), Ok(true))
                && !self
                    .services
                    .iter()
                    .any(|service| matches!(service.view.reader.contains_str(key), Ok(true)))
            {
                e.insert(value);
            }
        }
    }
}
```

> **Visibility notes (learned the hard way):** fields stay private; the constructor is `pub(crate)`;
> `FusedDataset` itself must be `pub` because it appears in the public `on_store` return type — exactly the
> precedent `DatasetView` already sets. `FusedService` stays `pub(crate)` with a `graph_name()` getter
> because `local_service.rs` needs to dedupe services by name (Step 3). If you make fields public you will
> trip clippy's `field_scoped_visibility_modifiers`.

**2b. The `QueryableDataset` impl — routing and stamping (the critical part):**

```rust
impl<'a> QueryableDataset<'a> for FusedDataset<'a> {
    type InternalTerm = EncodedTerm;
    type Error = StorageError;

    fn internal_quads_for_pattern(
        &self,
        subject: Option<&EncodedTerm>,
        predicate: Option<&EncodedTerm>,
        object: Option<&EncodedTerm>,
        graph_name: Option<Option<&EncodedTerm>>,
    ) -> impl Iterator<Item = Result<InternalQuad<EncodedTerm>, StorageError>> + use<'a> {
        // A virtual graph routes to the service store (its default graph); everything else
        // is delegated to the outer dataset (default graph, outer named graphs, and the
        // union-of-named-graphs case, which intentionally does not include service data).
        let iter: Box<dyn Iterator<Item = Result<InternalQuad<EncodedTerm>, StorageError>> + 'a> =
            if let Some(Some(graph_name)) = graph_name {
                if let Some(service) = self.find_service(graph_name) {
                    // The SERVICE's default graph is exposed as the virtual named graph:
                    // stamp the graph name on every quad so that graph-aware evaluation
                    // (e.g. quad patterns with an explicit graph name, pushed down by the
                    // optimizer) sees the graph it asked for.
                    let graph_name = graph_name.clone();
                    Box::new(
                        service
                            .internal_quads_for_pattern(subject, predicate, object, Some(None))
                            .map(move |quad| {
                                let mut quad = quad?;
                                quad.graph_name = Some(graph_name.clone());
                                Ok(quad)
                            }),
                    )
                } else {
                    Box::new(self.outer.internal_quads_for_pattern(
                        subject,
                        predicate,
                        object,
                        Some(Some(graph_name)),
                    ))
                }
            } else {
                Box::new(self.outer.internal_quads_for_pattern(
                    subject,
                    predicate,
                    object,
                    graph_name,
                ))
            };
        iter
    }

    fn internal_named_graphs(
        &self,
    ) -> impl Iterator<Item = Result<EncodedTerm, StorageError>> + use<'a> {
        // Virtual service graphs are intentionally NOT enumerated here: like the handler
        // path, `GRAPH ?g` only iterates the outer dataset's named graphs. Constant fused
        // graph names are still routed through `internal_quads_for_pattern` and
        // `contains_internal_graph_name`.
        self.outer.internal_named_graphs()
    }

    fn contains_internal_graph_name(&self, graph_name: &EncodedTerm) -> Result<bool, StorageError> {
        if self.find_service(graph_name).is_some() {
            Ok(true)
        } else {
            self.outer.contains_internal_graph_name(graph_name)
        }
    }

    fn internalize_term(&self, term: Term) -> Result<EncodedTerm, StorageError> {
        let encoded = (&term).into();
        insert_term(term, &encoded, &mut |key, value| self.insert_str(key, value));
        Ok(encoded)
    }

    fn externalize_term(&self, term: EncodedTerm) -> Result<Term, StorageError> {
        self.decode_term(&term)
    }

    fn externalize_expression_term(
        &self,
        term: EncodedTerm,
    ) -> Result<ExpressionTerm, StorageError> {
        Ok(match term {
            EncodedTerm::DefaultGraph => {
                return Err(CorruptionError::new("Unexpected default graph").into());
            }
            // ... inline literal fast paths, identical to DatasetView ...
            #[cfg(feature = "rdf-12")]
            EncodedTerm::Triple(t) => ExpressionTriple::new(
                self.externalize_expression_term(t.subject.clone())?,
                self.externalize_expression_term(t.predicate.clone())?,
                self.externalize_expression_term(t.object.clone())?,
            )
            .ok_or_else(|| CorruptionError::msg("Invalid triple term in the storage"))?
            .into(),
            _ => self.decode_term(&term)?.into(), // No escape
        })
    }

    // internalize_expression_term and internal_term_effective_boolean_value:
    // copy DatasetView's implementations VERBATIM (they are receiver-agnostic).
}
```

**Why the stamping exists (bug you would otherwise hit):** the optimizer pushes a constant graph name down
into the fused `GRAPH`'s inner quad patterns. spareval's quad-pattern evaluator then requires every returned
quad to carry `graph_name == Some(that_term)` and raises `QueryEvaluationError::UnexpectedDefaultGraph` when
it is `None`. Service stores return their *default graph* as `graph_name = None`, so without stamping the
first fused test fails with `UnexpectedDefaultGraph`. Stamping makes the returned quads match what the
evaluator asked for — semantically sound, since the stamped term *is* the graph being scanned.

**2c. The `StrLookup` impl (string resolution):**

```rust
impl StrLookup for FusedDataset<'_> {
    fn get_str(&self, key: &StrHash) -> Result<Option<OxString>, StorageError> {
        if let Some(value) = self.extra.borrow().get(key) {
            Ok(Some(value.clone()))
        } else if let Some(value) = self.outer.get_str(key)? {
            Ok(Some(value))
        } else {
            for service in &self.services {
                if let Some(value) = service.view.get_str(key)? {
                    return Ok(Some(value));
                }
            }
            Ok(None)
        }
    }
}
```

**Verify:** `cargo check -p oxigraph --all-features`. (Nothing uses `FusedDataset` yet — it only needs to
compile.)

---

### Step 3 — The `SERVICE → GRAPH` rewrite (`local_service.rs`)

**Goal:** walk the query algebra and replace every fusable `Service` node with a `Graph` node, opening stores
and registering `FusedService`s on the way.

**Imports to add** (two are easy to miss):

```rust
use crate::sparql::dataset::{DatasetView, FusedService};   // extend the existing DatasetView import
use crate::storage::StorageError;
use spareval::QueryableDataset;                              // for outer.internalize_term / contains_internal_graph_name
use spargebra::term::NamedNodePattern;
```

**3a. `fuse_query` — the entry point (all four query forms):**

```rust
/// Rewrites `SERVICE <oxigraph://...> { pattern }` patterns into virtual `GRAPH` patterns
/// evaluated in-process against the local stores, removing the service boundary from query
/// evaluation (fused dataset, see planning/service_boundary_design.md, R1).
///
/// A SERVICE node is fused only when:
/// - its name is a constant `oxigraph://` IRI,
/// - the store at that path can be opened read-only, and
/// - the outer dataset does not already contain a named graph with that IRI.
///
/// Otherwise the node is left untouched, so the regular service handler keeps its error and
/// SILENT semantics.
pub(crate) fn fuse_query(
    query: Query,
    handler: &LocalStoreServiceHandler,
    outer: &DatasetView<'_>,
    services: &mut Vec<FusedService>,
) -> Result<Query, StorageError> {
    Ok(match query {
        Query::Select(mut query) => {
            query.pattern = fuse_pattern(query.pattern, handler, outer, services)?;
            Query::Select(query)
        }
        Query::Construct(mut query) => {
            query.pattern = fuse_pattern(query.pattern, handler, outer, services)?;
            Query::Construct(query)
        }
        Query::Describe(mut query) => {
            query.pattern = fuse_pattern(query.pattern, handler, outer, services)?;
            Query::Describe(query)
        }
        Query::Ask(mut query) => {
            query.pattern = fuse_pattern(query.pattern, handler, outer, services)?;
            Query::Ask(query)
        }
    })
}
```

**3b. `fuse_pattern` — recursive rewrite (must be exhaustive over `spargebra::algebra::GraphPattern`):**

```rust
fn fuse_pattern(
    pattern: GraphPattern,
    handler: &LocalStoreServiceHandler,
    outer: &DatasetView<'_>,
    services: &mut Vec<FusedService>,
) -> Result<GraphPattern, StorageError> {
    Ok(match pattern {
        GraphPattern::Service { name, inner, silent } => {
            // A SERVICE whose inner pattern contains GRAPH nodes is not fused: such a pattern
            // addresses named graphs *of the service store*, while a fused GRAPH would route
            // them to the outer dataset. Keeping the SERVICE node preserves the handler semantics.
            // SILENT services are never fused either: a mid-evaluation storage error must stay
            // suppressed (the fallback handler path preserves this).
            let contains_graph = contains_graph_pattern(&inner);
            let inner = Box::new(fuse_pattern(*inner, handler, outer, services)?);
            let fused = !contains_graph
                && !silent
                && match &name {
                    NamedNodePattern::NamedNode(service_name) => {
                        try_fuse_service(service_name, handler, outer, services)?
                    }
                    NamedNodePattern::Variable(_) => false,
                };
            if fused {
                GraphPattern::Graph { name, inner }
            } else {
                GraphPattern::Service { name, inner, silent }
            }
        }
        GraphPattern::Bgp { patterns } => GraphPattern::Bgp { patterns },
        GraphPattern::Path { subject, path, object } => GraphPattern::Path { subject, path, object },
        GraphPattern::Join { left, right } => GraphPattern::Join {
            left: Box::new(fuse_pattern(*left, handler, outer, services)?),
            right: Box::new(fuse_pattern(*right, handler, outer, services)?),
        },
        GraphPattern::LeftJoin { left, right, expression } => GraphPattern::LeftJoin {
            left: Box::new(fuse_pattern(*left, handler, outer, services)?),
            right: Box::new(fuse_pattern(*right, handler, outer, services)?),
            expression,
        },
        GraphPattern::Lateral { left, right } => GraphPattern::Lateral {
            left: Box::new(fuse_pattern(*left, handler, outer, services)?),
            right: Box::new(fuse_pattern(*right, handler, outer, services)?),
        },
        GraphPattern::Filter { expr, inner } => GraphPattern::Filter {
            expr,
            inner: Box::new(fuse_pattern(*inner, handler, outer, services)?),
        },
        GraphPattern::Union { left, right } => GraphPattern::Union {
            left: Box::new(fuse_pattern(*left, handler, outer, services)?),
            right: Box::new(fuse_pattern(*right, handler, outer, services)?),
        },
        GraphPattern::Graph { name, inner } => GraphPattern::Graph {
            name,
            inner: Box::new(fuse_pattern(*inner, handler, outer, services)?),
        },
        GraphPattern::Extend { inner, variable, expression } => GraphPattern::Extend {
            inner: Box::new(fuse_pattern(*inner, handler, outer, services)?),
            variable,
            expression,
        },
        GraphPattern::Minus { left, right } => GraphPattern::Minus {
            left: Box::new(fuse_pattern(*left, handler, outer, services)?),
            right: Box::new(fuse_pattern(*right, handler, outer, services)?),
        },
        GraphPattern::Values { variables, bindings } => GraphPattern::Values { variables, bindings },
        GraphPattern::OrderBy { inner, expression } => GraphPattern::OrderBy {
            inner: Box::new(fuse_pattern(*inner, handler, outer, services)?),
            expression,
        },
        GraphPattern::Project { inner, variables } => GraphPattern::Project {
            inner: Box::new(fuse_pattern(*inner, handler, outer, services)?),
            variables,
        },
        GraphPattern::Distinct { inner } => GraphPattern::Distinct {
            inner: Box::new(fuse_pattern(*inner, handler, outer, services)?),
        },
        GraphPattern::Reduced { inner } => GraphPattern::Reduced {
            inner: Box::new(fuse_pattern(*inner, handler, outer, services)?),
        },
        GraphPattern::Slice { inner, start, length } => GraphPattern::Slice {
            inner: Box::new(fuse_pattern(*inner, handler, outer, services)?),
            start,
            length,
        },
        GraphPattern::Group { inner, variables, aggregates } => GraphPattern::Group {
            inner: Box::new(fuse_pattern(*inner, handler, outer, services)?),
            variables,
            aggregates,
        },
    })
}
```

> **Pitfall:** spargebra's `GraphPattern` has **no `#[non_exhaustive]`**-style hidden variants here — the match
> must be exhaustive. `Lateral` is **unconditional** in this spargebra version (oxigraph always enables the
> `sep-0006` feature), so do **not** put `#[cfg(feature = "sep-0006")]` on the `Lateral` arm — the arm would be
> compiled out, the match becomes non-exhaustive, and you get a confusing "non-exhaustive patterns" error that
> doesn't mention `Lateral` because the cfg'd arm is invisible.

**3c. `contains_graph_pattern` — the nested-GRAPH guard:**

```rust
/// Returns `true` when the pattern contains a `GRAPH` node (constant or variable name).
///
/// Used to decide whether a SERVICE node can be fused into a virtual graph: a fused SERVICE
/// exposes the service store's *default graph* as a named graph, so nested GRAPH nodes inside
/// the SERVICE would address the wrong store (see [`fuse_pattern`]).
fn contains_graph_pattern(pattern: &GraphPattern) -> bool {
    match pattern {
        GraphPattern::Graph { .. } => true,
        GraphPattern::Bgp { .. } | GraphPattern::Path { .. } | GraphPattern::Values { .. } => false,
        GraphPattern::Join { left, right }
        | GraphPattern::Union { left, right }
        | GraphPattern::Minus { left, right }
        | GraphPattern::LeftJoin { left, right, .. }
        | GraphPattern::Lateral { left, right } => {
            contains_graph_pattern(left) || contains_graph_pattern(right)
        }
        GraphPattern::Service { inner, .. }
        | GraphPattern::Filter { inner, .. }
        | GraphPattern::Extend { inner, .. }
        | GraphPattern::OrderBy { inner, .. }
        | GraphPattern::Project { inner, .. }
        | GraphPattern::Distinct { inner }
        | GraphPattern::Reduced { inner }
        | GraphPattern::Slice { inner, .. }
        | GraphPattern::Group { inner, .. } => contains_graph_pattern(inner),
    }
}
```

> **Clippy:** the `Service` and `Filter|…` arms have identical bodies — keep them merged as above or clippy
> will flag `if_same_then_else`-style duplication (the real lint fired was on the merged single-child arms).

**3d. `try_fuse_service` — open, check, register:**

```rust
/// Tries to open the local store behind an `oxigraph://` SERVICE IRI and record it as a
/// fused service graph. Returns `true` when the SERVICE node should be rewritten as a
/// `GRAPH` node.
fn try_fuse_service(
    service_name: &NamedNode,
    handler: &LocalStoreServiceHandler,
    outer: &DatasetView<'_>,
    services: &mut Vec<FusedService>,
) -> Result<bool, StorageError> {
    let Some(path) = service_name.as_str().strip_prefix(OXIGRAPH_SCHEME) else {
        return Ok(false);
    };
    let Ok(store) = handler.get_or_open_store(&PathBuf::from(path)) else {
        return Ok(false); // The store can't be opened: keep the SERVICE node (handler semantics)
    };
    let graph_name = outer.internalize_term(oxrdf::Term::NamedNode(service_name.clone()))?;
    if outer.contains_internal_graph_name(&graph_name)? {
        return Ok(false); // The outer dataset already has this named graph: keep the SERVICE node
    }
    if !services.iter().any(|service| service.graph_name() == &graph_name) {
        let reader = store.storage().snapshot();
        services.push(FusedService::new(graph_name, DatasetView::new(reader)));
    }
    Ok(true)
}
```

**Verify:** `cargo check -p oxigraph --all-features`. (Still unreferenced externally except by the next step.)

---

### Step 4 — Wiring (`sparql/mod.rs`)

**Goal:** a new evaluator builder, a new `PreparedSparqlQuery` field, the bind-time fusion call, and the
`on_store`/`on_transaction` return-type change.

**4a. Imports:**

```rust
use crate::sparql::dataset::{DatasetView, FusedDataset};   // add FusedDataset
use crate::sparql::local_service::fuse_query;               // new
```

**4b. `SparqlEvaluator` — new field, builder, default, pass-through:**

```rust
pub struct SparqlEvaluator {
    // ...existing fields...
    local_service_handler: Option<LocalStoreServiceHandler>,
}

impl SparqlEvaluator {
    /// Use a given [`LocalStoreServiceHandler`] to execute local `SERVICE` calls and fuse
    /// them into the query plan.
    ///
    /// Like [`with_default_service_handler`](Self::with_default_service_handler), but
    /// additionally enables *fused* execution of `SERVICE <oxigraph://...>` patterns: such
    /// patterns are rewritten into virtual `GRAPH` patterns evaluated in-process against the
    /// local stores, which removes the service boundary from evaluation (see the R1 design
    /// in `planning/service_boundary_design.md`). Stores that cannot be opened keep the
    /// regular service-handler path (errors and `SILENT` semantics are preserved).
    ///
    /// Fusion is skipped (the handler path is kept) for `SERVICE SILENT` calls, SERVICE calls
    /// whose inner pattern contains `GRAPH` nodes, and queries with `FROM NAMED` clauses, so
    /// their semantics are always preserved.
    ///
    /// Caveats of the fused path:
    /// - Terms flow between stores by their encoded form, which is a keyless hash of the term
    ///   string; blank nodes are the only non-portable encoding and may collide with blank
    ///   nodes of the outer dataset. Avoid fusing stores that contain blank nodes.
    /// - Local stores are opened (read-only) when the query is bound, even if the SERVICE
    ///   branch is never executed.
    ///
    /// See [`LocalStoreServiceHandler`] for an example.
    #[inline]
    pub fn with_local_service_handler(mut self, handler: LocalStoreServiceHandler) -> Self {
        #[cfg(feature = "http-client")]
        {
            self.with_http_default_service_handler = false;
        }
        self.local_service_handler = Some(handler.clone());
        self.inner = self.inner.with_default_service_handler(handler);
        self
    }
}

impl Default for SparqlEvaluator {
    fn default() -> Self {
        Self {
            // ...existing fields...
            local_service_handler: None,
        }
    }
}
```

> Registering the handler as the **default service handler too** (`self.inner.with_default_service_handler`)
> is deliberate: fused SERVICE nodes never reach the evaluator, but *un-fused* ones (guards, variable names)
> still need the handler. This mirrors `with_default_service_handler`.

**4c. `for_query` — carry the handler into the prepared query:**

```rust
pub fn for_query(self, query: Query) -> PreparedSparqlQuery {
    let dataset = query.dataset().cloned().map(Into::into).unwrap_or_default();
    let local_service_handler = self.local_service_handler.clone();
    PreparedSparqlQuery {
        dataset,
        query,
        evaluator: self.into_evaluator(),
        substitutions: HashMap::new(),
        local_service_handler,
    }
}
```

**4d. `PreparedSparqlQuery` — field, bind-time fusion, return types:**

```rust
pub struct PreparedSparqlQuery {
    // ...existing fields...
    local_service_handler: Option<LocalStoreServiceHandler>,
}

impl PreparedSparqlQuery {
    /// Bind the prepared query to the [`Store`] it should be evaluated on.
    pub fn on_store(self, store: &Store) -> BoundPreparedSparqlQuery<'static, FusedDataset<'static>> {
        let reader = store.storage().snapshot();
        let queryable_dataset = DatasetView::new(reader);
        self.bind_local_services(queryable_dataset)
    }

    /// Bind the prepared query to the [`Transaction`] it should be evaluated on.
    pub fn on_transaction<'b>(
        self,
        transaction: &'b Transaction<'_>,
    ) -> BoundPreparedSparqlQuery<'b, FusedDataset<'b>> {
        let reader = transaction.inner().reader();
        let dataset = DatasetView::new(reader);
        self.bind_local_services(dataset)
    }

    /// Binds the query to a dataset that fuses local `SERVICE <oxigraph://...>` patterns
    /// into virtual graphs evaluated in-process against the local stores.
    ///
    /// Queries with `FROM NAMED` clauses are not fused: the dataset specification gates named
    /// graph access, which would make the fused SERVICE return nothing (the handler path is
    /// independent of the dataset specification and keeps the SERVICE semantics).
    fn bind_local_services(
        mut self,
        queryable_dataset: DatasetView<'_>,
    ) -> BoundPreparedSparqlQuery<'_, FusedDataset<'_>> {
        let mut services = Vec::new();
        let has_from_named = self.query.dataset().is_some_and(|d| d.named.is_some());
        if let Some(handler) = &self.local_service_handler {
            if !has_from_named {
                match fuse_query(self.query.clone(), handler, &queryable_dataset, &mut services) {
                    Ok(fused_query) => self.query = fused_query,
                    Err(_) => services.clear(), // Keep the original query: SERVICE nodes use the handler path
                }
            }
        }
        let fused_dataset = FusedDataset::new(queryable_dataset, services);
        self.on_queryable_dataset(fused_dataset)
    }
}
```

**4e. Source compatibility check.** Every existing caller chains `…on_store(&store).execute()` (verified:
`cli/src/main.rs`, `python/src/store.rs`, `js/src/store.rs`, `fuzz/fuzz_targets/*.rs`,
`testsuite/src/sparql_evaluator.rs`, benches, and ~10 test sites). `BoundPreparedSparqlQuery<'a, D>` is
generic with `D = DatasetView<'a>` as the default parameter, so *named* type usages (`BoundPreparedSparqlQuery`
in type position) still compile; only the concrete return type changes. The one real risk is the
`private_interfaces`-adjacent visibility warning (Step 7a).

**Verify:**
```bash
cargo check -p oxigraph --all-features
# optional but cheap insurance against breaking callers in other crates:
cargo check --workspace --all-features 2>&1 | tail -5   # may be slow; cli/python/js are the interesting ones
```

---

### Step 5 — Tests (`tests/local_service.rs`)

**Goal:** prove fused == handler results, plus the guard semantics. Reuse the existing test harness
(`populated_store`, `COMPANIES`, `EMPLOYEES`; `drop(store)` before querying so the read-only open succeeds).

The six tests to add (see the shipped file for the full code):

| Test | What it verifies |
|---|---|
| `local_service_fused_matches_handler` | fused path returns exactly the handler path's results |
| `local_service_fused_federated_join` | two-store federated join through the fused path |
| `local_service_fused_silent_on_missing_store` | `SERVICE SILENT` on a missing store → one empty solution (fallback) |
| `local_service_fused_missing_store_is_error` | non-SILENT missing store → error in the stream (fallback) |
| `local_service_fused_outer_graph_collision` | outer named graph with the SERVICE IRI: `GRAPH` reads outer data, `SERVICE` reads the store, decoy never leaks |
| `local_service_fused_limit` | `LIMIT 1` produces exactly 1 solution |

Key snippets:

```rust
let evaluate = |fused: bool| -> Result<Vec<String>, Box<dyn Error>> {
    let evaluator = if fused {
        SparqlEvaluator::new().with_local_service_handler(LocalStoreServiceHandler::new())
    } else {
        SparqlEvaluator::new().with_default_service_handler(LocalStoreServiceHandler::new())
    };
    // ... parse_query(&query)?.on_store(&Store::new()?).execute()? ... collect labels
};
assert_eq!(fused_results, handler_results);
```

For the collision test, the outer store holds a decoy quad in `GRAPH <oxigraph://{dir_path}>`:

```rust
let graph_name = NamedNode::new(format!("oxigraph://{dir_path}"))?;
outer.insert(oxigraph::model::Quad::new(
    NamedNode::new("http://example.org/Decoy")?,
    NamedNode::new("http://example.org/name")?,
    oxigraph::model::Literal::new_simple_literal("DECOY"),
    graph_name.clone(),          // <-- graph name is the LAST argument of Quad::new
))?;
```

> **Test pitfalls (both real, both hit during development):**
> - `Quad::new(subject, predicate, object, graph_name)` — the **graph name is the 4th argument**, not the 1st.
> - `Quad::new` takes `impl Into` parameters: don't add explicit `.into()` calls on arguments; type inference
>   breaks. Write the values plainly.

**Verify:**
```bash
cargo test -p oxigraph --features rocksdb --test local_service
```
Expect the two equivalence tests to fail with `UnexpectedDefaultGraph` **until** the stamping from Step 2b
is in place — that failure is the expected signal that you've reached the stamping requirement.

---

### Step 6 — Benchmark (`benches/local_service.rs`)

**Goal:** measure fused vs handler vs direct, and assert `direct == handler == fused`.

- Add a `fused: bool` parameter to `run_queries` and dispatch:

```rust
fn run_queries(
    store: &Store,
    queries: &[Query],
    handler: Option<LocalStoreServiceHandler>,
    fused: bool,
) {
    let evaluator = match handler {
        Some(h) if fused => SparqlEvaluator::new().with_local_service_handler(h),
        Some(h) => SparqlEvaluator::new().with_default_service_handler(h),
        None => SparqlEvaluator::new(),
    };
    for query in queries {
        // ...evaluate and drain...
    }
}
```

- Add a third arm to the timing group: `group.bench_function("SERVICE fused", |b| b.iter(|| run_queries(&main_store, &service_queries, Some(handler.clone()), true)));`
- Strengthen the equivalence check to three-way: `if dc != sc || dc != fc { eprintln!("MISMATCH query {i}: direct={dc} service={sc} fused={fc}"); }` and `assert_eq!(dc, fc, ...)`, printing `N=…: correctness verified OK (direct == handler == fused)`.
- Check `criterion_main!` at the bottom: it is TEMP-limited to `(n1000, n5000)` on the working branch — restore `n25000, n100000` only when you want the long runs. The bench downloads `dataset-{N}.nt.bz2` from Zenodo on first run (cached in `lib/oxigraph/benches/data/`); later phases reuse it.

**Verify:**
```bash
cargo bench -p oxigraph --bench local_service   # allow a long first run; expect ~15–30 min for N=1000+5000
```

---

### Step 7 — Hardening: clippy, docs, full validation

**7a. Visibility (clippy `private_interfaces`-adjacent warning).** `FusedDataset` in a public return type but
`pub(crate)` → warning. Fix: make `FusedDataset` `pub` (precedent: `DatasetView`), keep its fields private,
keep methods `pub(crate)`. Same for `FusedService`: `pub(crate)` with a getter, private fields (clippy
`field_scoped_visibility_modifiers` fires if fields are `pub`).

**7b. Identical match arms.** In `contains_graph_pattern`, merge `Service` with the other single-child arms
(shown in Step 3c) or clippy flags the duplicated body.

**7c. Docs.** `with_local_service_handler` must document the blank-node caveat and the eager store open
(see the doc comment in Step 4b). Keep the `FusedDataset`/`FusedService` doc comments.

**7d. Final validation:**

```bash
cargo check -p oxigraph --all-features
cargo test -p oxigraph --features rocksdb --test local_service   # 18+ tests
cargo test -p oxigraph --features rocksdb                        # full suite (~121 tests)
cargo clippy -p oxigraph --all-features --all-targets            # no new warnings
```

---

## 4. Pitfalls & Debugging Reference (symptom → cause → fix)

| Symptom | Cause | Fix |
|---|---|---|
| Compile: `EncodedTerm` does not implement `Copy` | `EncodedTerm` is a big enum (inline small strings) — not `Copy` | `.clone()` the stamped `graph_name` before moving it into the closure |
| Compile: shadowed variable / moved value in the routing if-let | `if let Some(Some(graph_name)) = graph_name` borrows, then the stamping closure needs an owned copy | `let graph_name = graph_name.clone();` inside the service branch |
| Compile: "method `internalize_term` not found" in `local_service.rs` | `QueryableDataset` trait not in scope | `use spareval::QueryableDataset;` |
| Compile: "non-exhaustive patterns" on the `fuse_pattern`/`contains_graph_pattern` match, no obvious missing variant | `GraphPattern::Lateral` is **unconditional**; a `#[cfg(feature = "sep-0006")]` arm is compiled out and the error doesn't name it | Drop the `cfg`; match `Lateral { left, right }` plainly |
| Runtime: `UnexpectedDefaultGraph` on the first fused query | Optimizer pushes the graph name into the fused quad patterns; service default-graph scans return `graph_name = None` | **Stamping** (Step 2b): set `quad.graph_name = Some(graph_name.clone())` on every quad from a fused service |
| Wrong results: nested `GRAPH` inside a fused SERVICE reads outer data | A fused SERVICE exposes only the service *default graph*; nested `GRAPH` must address the service's named graphs | Guard 1: `contains_graph_pattern` → keep the `Service` node |
| Wrong results: `SERVICE SILENT` now surfaces storage errors | Fusion drops the `silent` flag | Guard 2: `!silent` blocks fusion |
| Wrong results: outer named graph with the SERVICE IRI is shadowed | `GRAPH <p>` would resolve to the fused service instead of the outer graph | Guard 6: `outer.contains_internal_graph_name` check in `try_fuse_service` |
| Empty results with `FROM NAMED` | Dataset spec gates named-graph access; fused `GRAPH` sees nothing | Guard 7: skip fusion when `d.named.is_some()` |
| Clippy `field_scoped_visibility_modifiers` | `FusedService`/`FusedDataset` fields are `pub` | Private fields + `pub(crate)` constructors/getters |
| Clippy "identical match arms" in `contains_graph_pattern` | `Service` arm body duplicates the `Filter|…` arm | Merge arms (Step 3c) |
| Clippy/visibility warning on `FusedDataset` | `pub(crate)` type in public return type | Make it `pub` like `DatasetView` |
| Test compile: `Quad::new` argument mismatch | Graph name is the **last** argument | `Quad::new(subject, predicate, object, graph_name)` |
| Test compile: type inference errors from explicit `.into()` | `Quad::new` takes `impl Into` | Drop the `.into()`s |
| Bench compile: `E0061` wrong number of args at a `run_queries` call site | Signature gained `fused: bool` | Update all call sites (`direct`, `SERVICE (3 stores)`, correctness block) |
| Two fused SERVICES on the same path open the store twice | No dedup | `try_fuse_service` checks `services.iter().any(|s| s.graph_name() == &graph_name)` before pushing |

---

## 5. Validation Checklist & Expected Results

```bash
# 1. Baseline sanity (before any change)
cargo check -p oxigraph --all-features
cargo test -p oxigraph --features rocksdb --test local_service

# 2. After each step
cargo check -p oxigraph --all-features

# 3. After Step 5
cargo test -p oxigraph --features rocksdb --test local_service        # 6 new tests, all green

# 4. After Step 6
cargo bench -p oxigraph --bench local_service                         # see numbers below

# 5. After Step 7
cargo test -p oxigraph --features rocksdb                             # ~121 tests
cargo clippy -p oxigraph --all-features --all-targets                 # no new warnings
```

Expected benchmark numbers (measured on `basenote-bench2`, 5 query pairs, `LIMIT 50`, sample_size 10,
±5 % machine variance; your absolute numbers will differ, the ratios are the point):

| N | direct (named graphs) | SERVICE (handler) | **SERVICE fused** | fused vs handler | fused vs direct |
|---|---|---|---|---|---|
| 1000 | 46.4 ms | 92.8 ms | **6.7 ms** | ~14× faster | ~7× faster |
| 5000 | 2.3 ms | 390 ms | **1.3 ms** | ~310× faster | ~1.8× faster |

The fused path even beats the single-store named-graphs path at N=5000: joins run on identity terms with no
boundary cost, and each store's index is smaller (better cache locality). Correctness is asserted in-bench:
`direct == handler == fused` for all query pairs at both sizes.

---

## 6. Follow-up Extensions (after this lands)

1. **Fused cardinality statistics** — extend the `StatisticsProvider` hook to virtual graph names so the
   optimizer gets real estimates for fused `GRAPH` nodes on larger multi-join queries. (Fusion currently
   bypasses `service_statistics`, which only answers for `Service` nodes.)
2. **Synchronous fallback (Tier 1)** — the eager baseline `handle()` is fine for correctness; if you want the
   fallback path to be lazy/thread-free, replace the `Vec` collect with a synchronous lazy iterator (the
   snapshot is `'static`, so `DatasetView<'static>` can be kept alive by the returned iterator).
3. **R2 — lateral SERVICE joins + binding pushdown** — with fusion in place, fused `GRAPH` nodes already pass
   `is_fit_for_for_loop_join`; a cost-gated lateral join would make highly selective `LIMIT` queries stop even
   earlier. Needs `ServiceContext.incoming_bindings` in spareval — the one piece of the later branch work this
   guide deliberately left out.
4. **Blank-node handling** — the one non-portable encoding. Options: refuse to fuse stores containing blank
   nodes (current documented behavior), or re-map blank-node hashes per store at bind time.
