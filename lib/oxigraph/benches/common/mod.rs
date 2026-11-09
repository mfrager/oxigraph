//! Benchmark bodies shared by the persistent backends.
//!
//! They are registered by `benches/store_slatedb.rs`, which runs every backend compiled in, so that
//! the RocksDB and SlateDB numbers of `planning/slatedb/performance.md` come from the same code.
//! Set `OXIGRAPH_BENCH_QUADS` to change the size of the load and deletion workloads.

use codspeed_criterion_compat::{BatchSize, Criterion, Throughput};
use oxigraph::model::*;
use oxigraph::sparql::{QueryResults, SparqlEvaluator};
use oxigraph::store::Store;
use std::hint::black_box;
use std::path::Path;
use tempfile::TempDir;

/// Number of quads of the workloads that are not about load throughput.
const SMALL_SIZE: usize = 1_000;
/// Number of quads inserted one by one, i.e. with one commit each. Kept small because a SlateDB
/// commit with the default `Wal` durability costs about one flush interval.
const INSERT_SIZE: usize = 100;
/// Number of quads of the load and graph deletion workloads.
const DEFAULT_LARGE_SIZE: usize = 100_000;

fn large_size() -> usize {
    std::env::var("OXIGRAPH_BENCH_QUADS")
        .ok()
        .and_then(|size| size.parse().ok())
        .unwrap_or(DEFAULT_LARGE_SIZE)
}

fn generated_quads(count: usize, graph_name: Option<&GraphName>) -> Vec<Quad> {
    (0..count)
        .map(|i| {
            Quad::new(
                NamedNode::new_unchecked(format!("http://example.com/s{i}")),
                NamedNode::new_unchecked(format!("http://example.com/p{}", i % 10)),
                Literal::new_simple_literal(format!("value{i}")),
                graph_name.cloned().unwrap_or(GraphName::DefaultGraph),
            )
        })
        .collect()
}

#[cfg(feature = "rocksdb")]
fn open_rocksdb(path: &Path) -> Store {
    Store::open(path).unwrap()
}

#[cfg(feature = "slatedb")]
fn open_slatedb(path: &Path) -> Store {
    Store::open_with_slate_db(oxigraph::store::SlateDbOptions::at_directory(path)).unwrap()
}

#[cfg(feature = "slatedb")]
fn open_slatedb_process(path: &Path) -> Store {
    Store::open_with_slate_db(
        oxigraph::store::SlateDbOptions::at_directory(path)
            .with_durability(oxigraph::store::SlateDbDurability::Process),
    )
    .unwrap()
}

/// Opens a store of every persistent backend compiled in, with its name.
fn persistent_backends() -> Vec<(&'static str, fn(&Path) -> Store)> {
    let backends: &[(&'static str, fn(&Path) -> Store)] = &[
        #[cfg(feature = "rocksdb")]
        ("rocksdb", open_rocksdb),
        #[cfg(feature = "slatedb")]
        ("slatedb", open_slatedb),
    ];
    backends.to_vec()
}

/// The backends of the single insert workload: it is the only one where the durability policy is
/// the dominant cost, so SlateDB's cheap policy is measured next to the default one.
fn insert_backends() -> Vec<(&'static str, fn(&Path) -> Store)> {
    let backends: &[(&'static str, fn(&Path) -> Store)] = &[
        #[cfg(feature = "slatedb")]
        ("slatedb Process durability", open_slatedb_process),
    ];
    backends.to_vec()
}

/// A freshly opened, empty store and the directory holding it.
fn fixture(open: fn(&Path) -> Store) -> (TempDir, Store) {
    let dir = TempDir::new().unwrap();
    let store = open(dir.path());
    (dir, store)
}

pub(crate) fn insert_quads(c: &mut Criterion) {
    let quads = generated_quads(INSERT_SIZE, None);
    let mut group = c.benchmark_group("phase3 insert single quads");
    group.sample_size(10);
    group.throughput(Throughput::Elements(INSERT_SIZE as u64));
    for (name, open) in [persistent_backends(), insert_backends()].concat() {
        group.bench_function(name, |b| {
            b.iter_batched(
                || fixture(open),
                |(_dir, store)| {
                    for quad in &quads {
                        store.insert(quad.clone()).unwrap();
                    }
                },
                BatchSize::PerIteration,
            );
        });
    }
    group.finish();
}

pub(crate) fn extend_quads(c: &mut Criterion) {
    let quads = generated_quads(10_000, None);
    let mut group = c.benchmark_group("phase3 extend 10k quads");
    group.sample_size(10);
    group.throughput(Throughput::Elements(quads.len() as u64));
    for (name, open) in persistent_backends() {
        group.bench_function(name, |b| {
            b.iter_batched(
                || fixture(open),
                |(_dir, store)| {
                    store.extend(quads.clone()).unwrap();
                },
                BatchSize::PerIteration,
            );
        });
    }
    group.finish();
}

pub(crate) fn bulk_load(c: &mut Criterion) {
    let size = large_size();
    let quads = generated_quads(size, None);
    let mut group = c.benchmark_group("phase3 bulk load");
    group.sample_size(10);
    group.throughput(Throughput::Elements(size as u64));
    for (name, open) in persistent_backends() {
        group.bench_function(name, |b| {
            b.iter_batched(
                || fixture(open),
                |(_dir, store)| {
                    let mut loader = store.bulk_loader();
                    loader.load_quads(quads.clone()).unwrap();
                    loader.commit().unwrap();
                },
                BatchSize::PerIteration,
            );
        });
    }
    #[cfg(feature = "slatedb")]
    group.bench_function("slatedb non atomic", |b| {
        b.iter_batched(
            || fixture(open_slatedb),
            |(_dir, store)| {
                let mut loader = store.bulk_loader().without_atomicity();
                loader.load_quads(quads.clone()).unwrap();
                loader.commit().unwrap();
            },
            BatchSize::PerIteration,
        );
    });
    group.finish();
}

fn populated(open: fn(&Path) -> Store, quads: &[Quad]) -> (TempDir, Store) {
    let (dir, store) = fixture(open);
    let mut loader = store.bulk_loader();
    loader.load_quads(quads.to_vec()).unwrap();
    loader.commit().unwrap();
    (dir, store)
}

pub(crate) fn point_lookup(c: &mut Criterion) {
    let quads = generated_quads(SMALL_SIZE, None);
    let mut group = c.benchmark_group("phase3 point lookup by subject");
    group.sample_size(10);
    group.throughput(Throughput::Elements(SMALL_SIZE as u64));
    for (name, open) in persistent_backends() {
        let (_dir, store) = populated(open, &quads);
        group.bench_function(name, |b| {
            b.iter(|| {
                for quad in &quads {
                    black_box(store.contains(quad).unwrap());
                }
            });
        });
    }
    group.finish();
}

pub(crate) fn predicate_scan(c: &mut Criterion) {
    let quads = generated_quads(SMALL_SIZE, None);
    let predicate = NamedNode::new_unchecked("http://example.com/p0");
    let mut group = c.benchmark_group("phase3 scan by predicate");
    group.sample_size(10);
    group.throughput(Throughput::Elements(SMALL_SIZE as u64 / 10));
    for (name, open) in persistent_backends() {
        let (_dir, store) = populated(open, &quads);
        group.bench_function(name, |b| {
            b.iter(|| {
                let count = store
                    .quads_for_pattern(None, Some(&predicate), None, None)
                    .count();
                black_box(count);
            });
        });
    }
    group.finish();
}

pub(crate) fn sparql_mix(c: &mut Criterion) {
    let quads = generated_quads(SMALL_SIZE, None);
    let queries = [
        "SELECT * WHERE { ?s ?p ?o }",
        "SELECT (COUNT(*) AS ?count) WHERE { ?s <http://example.com/p3> ?o }",
        "SELECT ?s ?o WHERE { ?s <http://example.com/p1> ?o } ORDER BY ?o LIMIT 10",
    ]
    .map(|query| SparqlEvaluator::new().parse_query(query).unwrap());
    let mut group = c.benchmark_group("phase3 sparql mix");
    group.sample_size(10);
    for (name, open) in persistent_backends() {
        let (_dir, store) = populated(open, &quads);
        group.bench_function(name, |b| {
            b.iter(|| {
                for query in &queries {
                    match query
                        .clone()
                        .on_store(&store)
                        .execute()
                        .unwrap()
                    {
                        QueryResults::Boolean(value) => {
                            black_box(value);
                        }
                        QueryResults::Solutions(solutions) => {
                            black_box(solutions.count());
                        }
                        QueryResults::Graph(triples) => {
                            black_box(triples.count());
                        }
                    }
                }
            });
        });
    }
    group.finish();
}

pub(crate) fn delete_graph(c: &mut Criterion) {
    let size = large_size();
    let graph_name = NamedNode::new_unchecked("http://example.com/g");
    let quads = generated_quads(size, Some(&graph_name.clone().into()));
    let mut group = c.benchmark_group("phase3 delete a named graph");
    group.sample_size(10);
    group.throughput(Throughput::Elements(size as u64));
    for (name, open) in persistent_backends() {
        group.bench_function(name, |b| {
            b.iter_batched(
                || populated(open, &quads),
                |(_dir, store)| {
                    store.clear_graph(&graph_name.clone().into()).unwrap();
                },
                BatchSize::PerIteration,
            );
        });
    }
    group.finish();
}

pub(crate) fn cold_reopen(c: &mut Criterion) {
    let size = large_size();
    let quads = generated_quads(size, None);
    let mut group = c.benchmark_group("phase3 cold reopen");
    group.sample_size(10);
    group.throughput(Throughput::Elements(size as u64));
    for (name, open) in persistent_backends() {
        group.bench_function(name, |b| {
            b.iter_batched(
                || {
                    let (dir, store) = populated(open, &quads);
                    drop(store);
                    dir
                },
                |dir| {
                    let store = open(dir.path());
                    black_box(store.len().unwrap());
                    drop(store);
                },
                BatchSize::PerIteration,
            );
        });
    }
    group.finish();
}
