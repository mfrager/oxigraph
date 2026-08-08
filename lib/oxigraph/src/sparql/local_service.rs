use crate::model::{NamedNode, OxString};
use crate::sparql::dataset::{DatasetView, FusedService};
use crate::storage::StorageError;
use crate::store::Store;
use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use oxiri::Iri;
use spareval::{DefaultServiceHandler, QueryEvaluationError, QueryEvaluator, QueryResults, QuerySolutionIter};
use spareval::QueryableDataset;
use spargebra::algebra::GraphPattern;
use spargebra::query::{Query, SelectQuery};
use spargebra::term::NamedNodePattern;
use std::path::{Path, PathBuf};
use std::sync::Arc;

const OXIGRAPH_SCHEME: &str = "oxigraph://";

/// A [`DefaultServiceHandler`] that resolves `oxigraph://` SERVICE IRIs to
/// local on-disk [`Store`] instances.
///
/// When a SPARQL query contains `SERVICE <oxigraph:///path/to/store> { ... }`,
/// this handler opens (or reuses) an on-disk [`Store`] at the given path and
/// evaluates the SERVICE pattern directly against it in-process, without any
/// HTTP round-trip.
///
/// Stores are cached internally so that repeated SERVICE calls to the same
/// path reuse the same underlying RocksDB database handle.
///
/// This requires the `rocksdb` feature and is not available on WASM.
///
/// # Example
///
/// ```no_run
/// use oxigraph::sparql::{LocalStoreServiceHandler, QueryResults, SparqlEvaluator};
/// use oxigraph::store::Store;
///
/// // Populate a target in-memory store for demonstration
/// let target = Store::new()?;
/// target.load_from_slice(
///     oxigraph::io::RdfFormat::NQuads,
///     "<http://example.com/s> <http://example.com/p> <http://example.com/o> .",
/// )?;
///
/// // Query with local federation (requires the rocksdb feature and an
/// // existing on-disk store at the SERVICE URI path)
/// let handler = LocalStoreServiceHandler::new();
/// if let QueryResults::Solutions(mut solutions) = SparqlEvaluator::new()
///     .with_default_service_handler(handler)
///     .parse_query(
///         "SELECT ?s ?o WHERE { SERVICE <oxigraph://target-store> { ?s ?p ?o } }",
///     )?
///     .on_store(&Store::new()?)
///     .execute()?
/// {
///     assert_eq!(solutions.next().unwrap()?.get("s").unwrap().to_string(), "http://example.com/s");
/// }
/// # Result::<_, Box<dyn std::error::Error>>::Ok(())
/// ```
#[derive(Clone)]
pub struct LocalStoreServiceHandler {
    stores: Arc<DashMap<PathBuf, Arc<Store>>>,
}

impl LocalStoreServiceHandler {
    /// Creates a new handler with an empty store cache.
    pub fn new() -> Self {
        Self {
            stores: Arc::new(DashMap::new()),
        }
    }

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

impl Default for LocalStoreServiceHandler {
    fn default() -> Self {
        Self::new()
    }
}

impl DefaultServiceHandler for LocalStoreServiceHandler {
    type Error = QueryEvaluationError;

    fn handle(
        &self,
        service_name: &NamedNode,
        pattern: &GraphPattern,
        base_iri: Option<&Iri<OxString>>,
    ) -> Result<QuerySolutionIter<'static>, Self::Error> {
        let iri_str = service_name.as_str();
        let path_str = iri_str
            .strip_prefix(OXIGRAPH_SCHEME)
            .ok_or_else(|| QueryEvaluationError::UnsupportedService(service_name.clone()))?;

        let path = PathBuf::from(path_str);

        // Get or open the store, caching it for reuse.
        // We open read-only so that concurrent queries (and a potential writer)
        // can coexist.
        let store = self.get_or_open_store(&path)?;

        // Create a DatasetView from a snapshot of the target store.
        let reader = store.storage().snapshot();
        let dataset = DatasetView::new(reader);

        // Build a SELECT query that captures every variable used in the
        // inner graph pattern, just like the HTTP federation path does.
        let query = Query::Select(SelectQuery {
            dataset: None,
            pattern: pattern.clone(),
            base_iri: base_iri.cloned(),
        });

        // Evaluate the pattern against the local store.
        // TODO: propagate the cancellation token from the parent evaluator
        // so that cancelling the outer query also interrupts local SERVICE calls.
        let evaluator = QueryEvaluator::new();
        let results = evaluator.prepare(&query).execute(dataset)?;

        match results {
            QueryResults::Solutions(iter) => {
                // Collect all results eagerly so the returned iterator is
                // 'static (the DatasetView snapshot is dropped here).
                let variables: Arc<[_]> = iter.variables().into();
                let solutions: Vec<_> = iter.collect();
                Ok(QuerySolutionIter::new(variables, solutions))
            }
            _ => Err(QueryEvaluationError::Service(
                "SERVICE pattern evaluation returned non-solution results".into(),
            )),
        }
    }
}

/// Open a store in read-only mode. Delegates to `Store::open_read_only`
/// when the `rocksdb` feature is active.
fn open_store_read_only(path: &Path) -> Result<Store, StorageError> {
    #[cfg(all(not(target_family = "wasm"), feature = "rocksdb"))]
    {
        Store::open_read_only(path)
    }
    #[cfg(not(all(not(target_family = "wasm"), feature = "rocksdb")))]
    {
        let _ = path;
        Err(StorageError::from(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "Opening on-disk stores requires the 'rocksdb' feature",
        )))
    }
}

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

/// Recursively rewrites the SERVICE nodes of a graph pattern (see [`fuse_query`]).
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
        GraphPattern::Path {
            subject,
            path,
            object,
        } => GraphPattern::Path {
            subject,
            path,
            object,
        },
        GraphPattern::Join { left, right } => GraphPattern::Join {
            left: Box::new(fuse_pattern(*left, handler, outer, services)?),
            right: Box::new(fuse_pattern(*right, handler, outer, services)?),
        },
        GraphPattern::LeftJoin {
            left,
            right,
            expression,
        } => GraphPattern::LeftJoin {
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
        GraphPattern::Extend {
            inner,
            variable,
            expression,
        } => GraphPattern::Extend {
            inner: Box::new(fuse_pattern(*inner, handler, outer, services)?),
            variable,
            expression,
        },
        GraphPattern::Minus { left, right } => GraphPattern::Minus {
            left: Box::new(fuse_pattern(*left, handler, outer, services)?),
            right: Box::new(fuse_pattern(*right, handler, outer, services)?),
        },
        GraphPattern::Values {
            variables,
            bindings,
        } => GraphPattern::Values {
            variables,
            bindings,
        },
        GraphPattern::OrderBy {
            inner,
            expression,
        } => GraphPattern::OrderBy {
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
        GraphPattern::Slice {
            inner,
            start,
            length,
        } => GraphPattern::Slice {
            inner: Box::new(fuse_pattern(*inner, handler, outer, services)?),
            start,
            length,
        },
        GraphPattern::Group {
            inner,
            variables,
            aggregates,
        } => GraphPattern::Group {
            inner: Box::new(fuse_pattern(*inner, handler, outer, services)?),
            variables,
            aggregates,
        },
    })
}

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
