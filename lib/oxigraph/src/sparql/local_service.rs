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
fn open_store_read_only(_path: &Path) -> Result<Store, crate::store::StorageError> {
    #[cfg(all(not(target_family = "wasm"), feature = "rocksdb"))]
    {
        Store::open_read_only(_path)
    }
    #[cfg(not(all(not(target_family = "wasm"), feature = "rocksdb")))]
    {
        Err(crate::store::StorageError::from(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "Opening on-disk stores requires the 'rocksdb' feature",
        )))
    }
}
