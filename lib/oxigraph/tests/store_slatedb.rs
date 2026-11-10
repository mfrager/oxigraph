#![cfg(test)]
#![cfg(all(not(target_family = "wasm"), feature = "slatedb"))]
#![expect(clippy::panic_in_result_fn)]

//! The on-disk subset of `tests/store.rs`, run against the SlateDB backend.

use oxigraph::io::RdfFormat;
use oxigraph::model::vocab::{rdf, xsd};
use oxigraph::model::*;
use oxigraph::sparql::{QueryResults, SparqlEvaluator};
use oxigraph::store::{SlateDbDurability, SlateDbOptions, StorageError, Store};
use std::error::Error;
use std::fs::{create_dir_all, remove_dir};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tempfile::TempDir;

#[expect(clippy::non_ascii_literal)]
const DATA: &str = r#"
@prefix schema: <http://schema.org/> .
@prefix wd: <http://www.wikidata.org/entity/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

wd:Q90 a schema:City ;
    schema:name "Paris"@fr , "la ville lumière"@fr ;
    schema:country wd:Q142 ;
    schema:population 2000000 ;
    schema:startDate "-300"^^xsd:gYear ;
    schema:url "https://www.paris.fr/"^^xsd:anyURI ;
    schema:postalCode "75001" .
"#;

#[expect(clippy::non_ascii_literal)]
const GRAPH_DATA: &str = r#"
@prefix schema: <http://schema.org/> .
@prefix wd: <http://www.wikidata.org/entity/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

GRAPH <http://www.wikidata.org/wiki/Special:EntityData/Q90> {
    wd:Q90 a schema:City ;
        schema:name "Paris"@fr , "la ville lumière"@fr ;
        schema:country wd:Q142 ;
        schema:population 2000000 ;
        schema:startDate "-300"^^xsd:gYear ;
        schema:url "https://www.paris.fr/"^^xsd:anyURI ;
        schema:postalCode "75001" .
}
"#;
const NUMBER_OF_TRIPLES: usize = 8;

fn open(directory: &TempDir) -> Result<Store, Box<dyn Error>> {
    Ok(Store::open_with_slate_db(SlateDbOptions::at_directory(
        directory.path(),
    ))?)
}

fn quads(graph_name: impl Into<GraphName>) -> Vec<Quad> {
    let graph_name = graph_name.into();
    let paris = NamedNode::new_unchecked("http://www.wikidata.org/entity/Q90");
    let france = NamedNode::new_unchecked("http://www.wikidata.org/entity/Q142");
    let city = NamedNode::new_unchecked("http://schema.org/City");
    let name = NamedNode::new_unchecked("http://schema.org/name");
    let country = NamedNode::new_unchecked("http://schema.org/country");
    let population = NamedNode::new_unchecked("http://schema.org/population");
    let start_date = NamedNode::new_unchecked("http://schema.org/startDate");
    let url = NamedNode::new_unchecked("http://schema.org/url");
    let postal_code = NamedNode::new_unchecked("http://schema.org/postalCode");
    vec![
        Quad::new(paris.clone(), rdf::TYPE, city, graph_name.clone()),
        Quad::new(
            paris.clone(),
            name.clone(),
            Literal::new_language_tagged_literal_unchecked("Paris", "fr"),
            graph_name.clone(),
        ),
        Quad::new(
            paris.clone(),
            name,
            Literal::new_language_tagged_literal_unchecked("la ville lumi\u{E8}re", "fr"),
            graph_name.clone(),
        ),
        Quad::new(paris.clone(), country, france, graph_name.clone()),
        Quad::new(
            paris.clone(),
            population,
            Literal::new_typed_literal("2000000", xsd::INTEGER),
            graph_name.clone(),
        ),
        Quad::new(
            paris.clone(),
            start_date,
            Literal::new_typed_literal("-300", xsd::G_YEAR),
            graph_name.clone(),
        ),
        Quad::new(
            paris.clone(),
            url,
            Literal::new_typed_literal("https://www.paris.fr/", xsd::ANY_URI),
            graph_name.clone(),
        ),
        Quad::new(
            paris.clone(),
            postal_code,
            Literal::new_simple_literal("75001"),
            graph_name,
        ),
    ]
}

#[test]
fn test_load_graph_on_disk() -> Result<(), Box<dyn Error>> {
    let dir = TempDir::new()?;
    let store = open(&dir)?;
    store.load_from_reader(RdfFormat::Turtle, DATA.as_bytes())?;
    for q in quads(GraphName::DefaultGraph) {
        assert!(store.contains(&q)?);
    }
    store.validate()?;
    Ok(())
}

#[test]
fn test_load_dataset_on_disk() -> Result<(), Box<dyn Error>> {
    let dir = TempDir::new()?;
    let store = open(&dir)?;
    store.load_from_reader(RdfFormat::TriG, GRAPH_DATA.as_bytes())?;
    for q in quads(NamedNode::new_unchecked(
        "http://www.wikidata.org/wiki/Special:EntityData/Q90",
    )) {
        assert!(store.contains(&q)?);
    }
    store.validate()?;
    Ok(())
}

#[test]
fn test_bulk_load_graph_on_disk() -> Result<(), Box<dyn Error>> {
    let dir = TempDir::new()?;
    let store = open(&dir)?;
    let mut loader = store.bulk_loader();
    loader.load_from_slice(RdfFormat::Turtle, DATA.as_bytes())?;
    loader.commit()?;
    for q in quads(GraphName::DefaultGraph) {
        assert!(store.contains(&q)?);
    }
    store.validate()?;
    Ok(())
}

#[test]
fn test_bulk_load_dataset_on_disk() -> Result<(), Box<dyn Error>> {
    let dir = TempDir::new()?;
    let store = open(&dir)?;
    let mut loader = store.bulk_loader();
    loader.load_from_slice(RdfFormat::TriG, GRAPH_DATA.as_bytes())?;
    loader.commit()?;
    let graph_name = NamedNode::new_unchecked("http://www.wikidata.org/wiki/Special:EntityData/Q90");
    for q in quads(graph_name.clone()) {
        assert!(store.contains(&q)?);
    }
    assert!(store.contains_named_graph(&graph_name.into())?);
    store.validate()?;
    Ok(())
}

#[test]
fn test_bulk_load_is_atomic_on_disk() -> Result<(), Box<dyn Error>> {
    let dir = TempDir::new()?;
    let store = open(&dir)?;
    let mut loader = store.bulk_loader();
    loader.load_from_slice(RdfFormat::Turtle, DATA.as_bytes())?;
    // Dropping the loader without committing must not write anything.
    drop(loader);
    assert!(store.is_empty()?);
    store.validate()?;
    Ok(())
}

/// Copies a pinned fixture directory into `directory`, so that opening the database does not
/// modify the fixture itself.
fn copy_fixture(fixture: &str, directory: &TempDir) -> Result<(), Box<dyn Error>> {
    let source = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join(fixture);
    create_dir_all(directory.path())?;
    for entry in std::fs::read_dir(source)? {
        let entry = entry?;
        let target = directory.path().join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_directory(&entry.path(), &target)?;
        } else {
            std::fs::copy(entry.path(), target)?;
        }
    }
    Ok(())
}

fn copy_directory(from: &Path, to: &Path) -> Result<(), Box<dyn Error>> {
    create_dir_all(to)?;
    for entry in std::fs::read_dir(from)? {
        let entry = entry?;
        let target = to.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_directory(&entry.path(), &target)?;
        } else {
            std::fs::copy(entry.path(), target)?;
        }
    }
    Ok(())
}

/// The N-Quads dump of a store, with its lines sorted so that two backends can be compared.
fn sorted_dump(store: &Store) -> Result<Vec<String>, Box<dyn Error>> {
    let mut buffer = Vec::new();
    store.dump_to_writer(RdfFormat::NQuads, &mut buffer)?;
    let mut lines = String::from_utf8(buffer)?
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    lines.sort();
    Ok(lines)
}

/// Measures what a query iterator held for longer than SlateDB's garbage collection age sees
/// (planning/slatedb/implementation_plan2.md, Q8).
///
/// The default `wal_options.min_age` is 60 s: this test holds a `Store::iter()` open while the
/// database is written to and the garbage collector runs, then reads from it and reports whether
/// the quads are still there. It is ignored because it takes more than a minute; run it with
/// `cargo test -p oxigraph --features slatedb --test store_slatedb -- --ignored --nocapture
/// measure_a_held_iterator`.
#[test]
#[ignore = "takes more than a minute: measurement of the Q8 snapshot lifetime question"]
#[expect(clippy::print_stderr, reason = "the measurement is its output")]
fn measure_a_held_iterator() -> Result<(), Box<dyn Error>> {
    let dir = TempDir::new()?;
    let store = open(&dir)?;
    store.extend(generated_quads(2_000, None))?;
    store.flush()?;

    // The iterator takes a snapshot of the database, as every query does.
    let mut held = store.iter();
    let first = held.next().transpose()?;
    assert!(first.is_some());

    // Keep writing and flushing so that the compactor and the garbage collector have work.
    let started = Instant::now();
    for round in 0..6 {
        store.extend(generated_quads(500, None))?;
        store.flush()?;
        eprintln!(
            "round {round}: {} quads, {:.1}s",
            store.len()?,
            started.elapsed().as_secs_f64()
        );
        std::thread::sleep(Duration::from_secs(12));
    }
    eprintln!("held for {:.1}s", started.elapsed().as_secs_f64());

    // Does the held snapshot still see the data it was created with?
    let mut seen = usize::from(first.is_some());
    let mut error = None;
    for quad in held {
        match quad {
            Ok(_) => seen += 1,
            Err(error_) => {
                error = Some(error_.to_string());
                break;
            }
        }
    }
    match &error {
        Some(error) => {
            eprintln!("after the wait, the held iterator produced {seen} quads, error: {error}");
        }
        None => eprintln!("after the wait, the held iterator produced {seen} quads, no error"),
    }
    assert!(seen > 0);
    Ok(())
}

#[test]
fn test_dump_matches_the_other_backends() -> Result<(), Box<dyn Error>> {
    let memory = Store::new()?;
    let dir = TempDir::new()?;
    let slatedb = open(&dir)?;
    #[cfg(feature = "rocksdb")]
    let rocksdb_dir = TempDir::new()?;
    #[cfg(feature = "rocksdb")]
    let rocksdb = Store::open(&rocksdb_dir)?;
    let backends: Vec<(&str, &Store)> = [
        ("memory", &memory),
        ("SlateDB", &slatedb),
        #[cfg(feature = "rocksdb")]
        ("RocksDB", &rocksdb),
    ]
    .into_iter()
    .collect();

    for (name, store) in &backends {
        store.load_from_reader(RdfFormat::TriG, GRAPH_DATA.as_bytes())?;
        store.load_from_reader(RdfFormat::Turtle, DATA.as_bytes())?;
        assert_eq!(store.len()?, NUMBER_OF_TRIPLES * 2, "{name}");
    }
    let expected = sorted_dump(&memory)?;
    for (name, store) in &backends[1..] {
        assert_eq!(expected, sorted_dump(store)?, "the {name} dump differs");
    }
    slatedb.validate()?;
    Ok(())
}

#[test]
fn test_slatedb_backward_compatibility() -> Result<(), Box<dyn Error>> {
    // The fixture is a database created by an earlier build of this backend: opening it must keep
    // working, which is what catches an accidental key layout or format version change (§8.1).
    let dir = TempDir::new()?;
    copy_fixture("slatedb_fixture_data", &dir)?;
    // We run twice to check that the database is properly saved and closed.
    for _ in 0..2 {
        let store = open(&dir)?;
        for q in quads(GraphName::DefaultGraph) {
            assert!(store.contains(&q)?);
        }
        let graph_name =
            NamedNode::new_unchecked("http://www.wikidata.org/wiki/Special:EntityData/Q90");
        for q in quads(graph_name.clone()) {
            assert!(store.contains(&q)?);
        }
        assert!(store.contains_named_graph(&graph_name.into())?);
        assert_eq!(store.len()?, NUMBER_OF_TRIPLES * 2);
    }
    Ok(())
}

#[test]
#[cfg(feature = "rdf-12")]
fn test_slatedb_rdf_star_backward_compatibility() -> Result<(), Box<dyn Error>> {
    let dir = TempDir::new()?;
    copy_fixture("slatedb_bc_rdf_star_data", &dir)?;
    let s = NamedNode::new_unchecked("http://example.com/s");
    let p = NamedNode::new_unchecked("http://example.com/p");
    let o = NamedNode::new_unchecked("http://example.com/o");
    let g = NamedNode::new_unchecked("http://example.com/g");
    let bnode = BlankNode::new_unchecked("f2fef82410957224105241225fd0a648");
    let triple = Term::Triple(Triple::new(s.clone(), p.clone(), o.clone()).into());
    for _ in 0..2 {
        let store = open(&dir)?;
        for graph in [GraphName::from(g.clone()), GraphName::DefaultGraph] {
            assert!(store.contains(&Quad::new(s.clone(), p.clone(), o.clone(), graph.clone()))?);
            assert!(store.contains(&Quad::new(
                bnode.clone(),
                p.clone(),
                o.clone(),
                graph.clone()
            ))?);
            assert!(store.contains(&Quad::new(
                s.clone(),
                p.clone(),
                bnode.clone(),
                graph.clone()
            ))?);
            assert!(store.contains(&Quad::new(
                bnode.clone(),
                rdf::REIFIES,
                triple.clone(),
                graph.clone()
            ))?);
        }
        assert_eq!(store.len()?, 8);
    }
    Ok(())
}

#[test]
fn test_dump_graph_on_disk() -> Result<(), Box<dyn Error>> {
    let dir = TempDir::new()?;
    let store = open(&dir)?;
    for q in quads(GraphName::DefaultGraph) {
        store.insert(q)?;
    }

    let mut buffer = Vec::new();
    store.dump_graph_to_writer(&GraphName::DefaultGraph, RdfFormat::NTriples, &mut buffer)?;
    assert_eq!(
        buffer.into_iter().filter(|c| *c == b'\n').count(),
        NUMBER_OF_TRIPLES
    );
    Ok(())
}

#[test]
fn test_dump_dataset_on_disk() -> Result<(), Box<dyn Error>> {
    let dir = TempDir::new()?;
    let store = open(&dir)?;
    for q in quads(GraphName::DefaultGraph) {
        store.insert(q)?;
    }

    let buffer = store.dump_to_writer(RdfFormat::NQuads, Vec::new())?;
    assert_eq!(
        buffer.into_iter().filter(|c| *c == b'\n').count(),
        NUMBER_OF_TRIPLES
    );
    Ok(())
}

#[test]
fn test_snapshot_isolation_iterator_on_disk() -> Result<(), Box<dyn Error>> {
    let quad = Quad::new(
        NamedNode::new("http://example.com/s")?,
        NamedNode::new("http://example.com/p")?,
        NamedNode::new("http://example.com/o")?,
        NamedNode::new("http://www.wikidata.org/wiki/Special:EntityData/Q90")?,
    );
    let dir = TempDir::new()?;
    let store = open(&dir)?;
    store.insert(quad.clone())?;
    let iter = store.iter();
    store.remove(&quad)?;
    assert_eq!(iter.collect::<Result<Vec<_>, _>>()?, vec![quad]);
    store.validate()?;
    Ok(())
}

#[test]
fn test_read_your_own_write_transaction_on_disk() -> Result<(), Box<dyn Error>> {
    let store_dir = TempDir::new()?;
    let quad = Quad::new(
        NamedNode::new_unchecked("http://example.com/s"),
        NamedNode::new_unchecked("http://example.com/p"),
        NamedNode::new_unchecked("http://example.com/o"),
        GraphName::DefaultGraph,
    );

    let store = open(&store_dir)?;
    let mut transaction = store.start_transaction()?;
    transaction.insert(quad.clone());
    assert_eq!(transaction.iter().collect::<Result<Vec<_>, _>>()?, [quad]);

    Ok(())
}

#[test]
fn test_clear_graph_on_disk() -> Result<(), Box<dyn Error>> {
    let dir = TempDir::new()?;
    let store = open(&dir)?;
    let graph_name = NamedNode::new_unchecked("http://example.com/g");
    for q in quads(graph_name.clone()) {
        store.insert(q)?;
    }
    for q in quads(GraphName::DefaultGraph) {
        store.insert(q)?;
    }

    store.clear_graph(&graph_name.clone().into())?;
    // Like the RocksDB backend, clearing a named graph keeps it in the store, only empty.
    assert!(store.contains_named_graph(&graph_name.clone().into())?);
    assert!(
        store
            .quads_for_pattern(None, None, None, Some(&graph_name.into()))
            .next()
            .transpose()?
            .is_none(),
        "the cleared graph is still readable"
    );
    assert_eq!(store.len()?, NUMBER_OF_TRIPLES);
    store.validate()?;
    Ok(())
}

#[test]
fn test_remove_named_graph_on_disk() -> Result<(), Box<dyn Error>> {
    let dir = TempDir::new()?;
    let store = open(&dir)?;
    let graph_name = NamedNode::new_unchecked("http://example.com/g");
    for q in quads(graph_name.clone()) {
        store.insert(q)?;
    }
    store.insert(Quad::new(
        NamedNode::new_unchecked("http://example.com/s"),
        NamedNode::new_unchecked("http://example.com/p"),
        NamedNode::new_unchecked("http://example.com/o"),
        GraphName::DefaultGraph,
    ))?;

    store.remove_named_graph(&graph_name.clone().into())?;
    assert!(!store.contains_named_graph(&graph_name.clone().into())?);
    assert!(
        store
            .quads_for_pattern(None, None, None, Some(&graph_name.into()))
            .next()
            .transpose()?
            .is_none(),
        "the named graph is still readable"
    );
    assert_eq!(store.len()?, 1);
    store.validate()?;
    Ok(())
}

#[test]
fn test_clear_on_disk() -> Result<(), Box<dyn Error>> {
    let dir = TempDir::new()?;
    let store = open(&dir)?;
    for q in quads(NamedNode::new_unchecked("http://example.com/g")) {
        store.insert(q)?;
    }
    for q in quads(GraphName::DefaultGraph) {
        store.insert(q)?;
    }

    store.clear()?;
    assert!(store.is_empty()?);
    store.validate()?;
    Ok(())
}

#[test]
fn test_data_survives_a_reopen() -> Result<(), Box<dyn Error>> {
    let dir = TempDir::new()?;
    {
        let store = open(&dir)?;
        for q in quads(GraphName::DefaultGraph) {
            store.insert(q)?;
        }
        store.flush()?;
    }
    let store = open(&dir)?;
    assert_eq!(store.len()?, NUMBER_OF_TRIPLES);
    for q in quads(GraphName::DefaultGraph) {
        assert!(store.contains(&q)?);
    }
    store.validate()?;
    Ok(())
}

fn query_rows(store: &Store, query: &str) -> Result<Vec<String>, Box<dyn Error>> {
    let QueryResults::Solutions(solutions) = SparqlEvaluator::new()
        .parse_query(query)?
        .on_store(store)
        .execute()?
    else {
        return Err("the query should return solutions".into());
    };
    let mut rows = solutions
        .map(|solution| {
            solution.map(|solution| {
                solution
                    .iter()
                    .map(|(variable, value)| format!("{variable}={value}"))
                    .collect::<Vec<_>>()
                    .join(" ")
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    rows.sort();
    Ok(rows)
}

#[test]
fn test_sparql_results_match_the_memory_backend() -> Result<(), Box<dyn Error>> {
    let dir = TempDir::new()?;
    let memory = Store::new()?;
    let on_disk = open(&dir)?;
    for store in [&memory, &on_disk] {
        store.load_from_reader(RdfFormat::TriG, GRAPH_DATA.as_bytes())?;
    }

    let queries = [
        "SELECT * WHERE { ?s ?p ?o }",
        "SELECT * WHERE { GRAPH ?g { ?s ?p ?o } }",
        "SELECT (COUNT(*) AS ?count) WHERE { ?s ?p ?o }",
        "SELECT ?s WHERE { ?s <http://schema.org/name> \"Paris\"@fr }",
        "SELECT ?o WHERE { ?s <http://schema.org/population> ?o FILTER(?o > 1000) }",
        "SELECT ?s WHERE { ?s <http://schema.org/postalCode> ?c } ORDER BY ?c",
    ];
    for query in queries {
        assert_eq!(
            query_rows(&memory, query)?,
            query_rows(&on_disk, query)?,
            "the query {query} does not return the same solutions as the memory backend"
        );
    }
    on_disk.validate()?;

    #[cfg(feature = "rocksdb")]
    {
        let rocksdb_dir = TempDir::new()?;
        let rocksdb = Store::open(&rocksdb_dir)?;
        rocksdb.load_from_reader(RdfFormat::TriG, GRAPH_DATA.as_bytes())?;
        for query in queries {
            assert_eq!(
                query_rows(&memory, query)?,
                query_rows(&rocksdb, query)?,
                "the query {query} does not return the same solutions as the RocksDB backend"
            );
        }
        rocksdb.validate()?;
    }
    Ok(())
}

#[test]
#[cfg(not(feature = "rocksdb"))]
fn test_open_uses_slatedb_when_it_is_the_only_backend() -> Result<(), Box<dyn Error>> {
    let dir = TempDir::new()?;
    let quad = Quad::new(
        NamedNode::new_unchecked("http://example.com/s"),
        NamedNode::new_unchecked("http://example.com/p"),
        NamedNode::new_unchecked("http://example.com/o"),
        GraphName::DefaultGraph,
    );
    {
        let store = Store::open(&dir)?;
        store.insert(quad.clone())?;
    }
    let store = Store::open(&dir)?;
    assert!(store.contains(&quad)?);
    store.validate()?;
    Ok(())
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

#[test]
fn test_bulk_load_rollback() -> Result<(), Box<dyn Error>> {
    let dir = TempDir::new()?;
    let store = open(&dir)?;
    let mut loader = store.bulk_loader();
    loader.load_quads(generated_quads(100, None))?;
    drop(loader);
    assert!(store.is_empty()?);
    store.validate()?;
    drop(store);
    let store = open(&dir)?;
    assert!(store.is_empty()?);
    store.validate()?;
    Ok(())
}

#[test]
fn test_bulk_load_empty() -> Result<(), Box<dyn Error>> {
    let dir = TempDir::new()?;
    let store = open(&dir)?;
    let mut loader = store.bulk_loader();
    loader.load_quads(std::iter::empty::<Quad>())?;
    loader.commit()?;
    assert!(store.is_empty()?);
    store.validate()?;
    Ok(())
}

#[test]
fn test_bulk_load_on_existing_delete_overrides_the_delete() -> Result<(), Box<dyn Error>> {
    let quad = Quad::new(
        NamedNode::new_unchecked("http://example.com/s"),
        NamedNode::new_unchecked("http://example.com/p"),
        NamedNode::new_unchecked("http://example.com/o"),
        NamedNode::new_unchecked("http://www.wikidata.org/wiki/Special:EntityData/Q90"),
    );
    let dir = TempDir::new()?;
    let store = open(&dir)?;
    store.remove(&quad)?;
    let mut loader = store.bulk_loader();
    loader.load_quads([quad.clone()])?;
    loader.commit()?;
    assert_eq!(store.len()?, 1);
    assert!(store.contains(&quad)?);
    store.validate()?;
    Ok(())
}

#[test]
fn test_parallel_bulk_load_on_disk() -> Result<(), Box<dyn Error>> {
    let dir = TempDir::new()?;
    let store = open(&dir)?;
    let quads = generated_quads(5_000, None);
    let mut loader = store
        .bulk_loader()
        .with_num_threads(4)
        // 1 MB per thread is 1000 quads per batch, so the load goes through several batches and
        // several encoder threads.
        .with_max_memory_size_in_megabytes(1);
    loader.load_quads(quads.clone())?;
    loader.commit()?;
    assert_eq!(store.len()?, quads.len());
    for quad in &quads {
        assert!(store.contains(quad)?);
    }
    store.validate()?;
    Ok(())
}

#[test]
fn test_atomic_bulk_load_exceeds_budget_errors() -> Result<(), Box<dyn Error>> {
    let dir = TempDir::new()?;
    let store = Store::open_with_slate_db(
        SlateDbOptions::at_directory(dir.path()).with_bulk_load_max_staged_bytes(1),
    )?;
    let mut loader = store.bulk_loader();
    let error = expect_storage_error(
        loader
            .load_quads(generated_quads(10, None))
            .and_then(|()| loader.commit()),
    );
    assert!(
        error.to_string().contains("without_atomicity"),
        "unexpected error: {error}"
    );
    drop(store);
    let store = open(&dir)?;
    assert!(store.is_empty()?);
    store.validate()?;
    Ok(())
}

#[test]
fn test_non_atomic_bulk_load_is_visible_early() -> Result<(), Box<dyn Error>> {
    let dir = TempDir::new()?;
    let store = open(&dir)?;
    let mut loader = store
        .bulk_loader()
        .without_atomicity()
        .with_num_threads(1)
        // 1 MB for a single thread is 1000 quads per batch.
        .with_max_memory_size_in_megabytes(1);
    loader.load_quads(generated_quads(2_000, None))?;
    // The first batch has been encoded and written by now, the second one is still being encoded.
    assert_eq!(store.len()?, 1_000);
    assert!(store.contains(&generated_quads(1, None)[0])?);
    loader.commit()?;
    assert_eq!(store.len()?, 2_000);
    store.validate()?;
    Ok(())
}

#[test]
fn test_clear_is_not_visible_to_an_existing_reader() -> Result<(), Box<dyn Error>> {
    let dir = TempDir::new()?;
    let store = open(&dir)?;
    let graph_name = NamedNode::new_unchecked("http://example.com/g");
    let named_quads = quads(graph_name.clone());
    for quad in &named_quads {
        store.insert(quad.clone())?;
    }

    let before_clear = store.iter();
    store.clear_graph(&graph_name.clone().into())?;
    let mut seen = before_clear
        .map(|quad| quad.map(|quad| quad.to_string()))
        .collect::<Result<Vec<_>, _>>()?;
    seen.sort();
    let mut expected = named_quads
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    expected.sort();
    assert_eq!(
        seen, expected,
        "a reader opened before the clear must still see the graph"
    );
    assert!(
        store.quads_for_pattern(None, None, None, Some(&graph_name.into())).next().transpose()?.is_none(),
        "a new reader must not see the cleared graph"
    );
    store.validate()?;
    Ok(())
}

#[test]
fn test_clear_above_the_limit_errors() -> Result<(), Box<dyn Error>> {
    let dir = TempDir::new()?;
    let store = Store::open_with_slate_db(
        SlateDbOptions::at_directory(dir.path()).with_max_clear_deletes(4),
    )?;
    for quad in generated_quads(8, None) {
        store.insert(quad)?;
    }
    let error = expect_storage_error(store.clear());
    assert!(
        error.to_string().contains("max_clear_deletes"),
        "unexpected error: {error}"
    );
    assert_eq!(store.len()?, 8, "a refused clear must not delete anything");
    store.validate()?;
    Ok(())
}

#[test]
fn test_backup() -> Result<(), Box<dyn Error>> {
    let quad = Quad::new(
        NamedNode::new_unchecked("http://example.com/s"),
        NamedNode::new_unchecked("http://example.com/p"),
        NamedNode::new_unchecked("http://example.com/o"),
        GraphName::DefaultGraph,
    );
    let store_dir = TempDir::new()?;
    let backup_dir = TempDir::new()?;
    remove_dir(&backup_dir)?;
    let backup_of_backup_dir = TempDir::new()?;
    remove_dir(&backup_of_backup_dir)?;

    let store = open(&store_dir)?;
    store.insert(quad.clone())?;
    store.backup(&backup_dir)?;
    store.remove(&quad)?;
    assert!(!store.contains(&quad)?);

    let backup = open(&backup_dir)?;
    backup.validate()?;
    assert!(backup.contains(&quad)?);
    backup.backup(&backup_of_backup_dir)?;
    drop(backup);

    let backup_of_backup = open(&backup_of_backup_dir)?;
    backup_of_backup.validate()?;
    assert!(backup_of_backup.contains(&quad)?);
    Ok(())
}

#[test]
fn test_bad_backup() -> Result<(), Box<dyn Error>> {
    let store_dir = TempDir::new()?;
    let backup_dir = TempDir::new()?;

    create_dir_all(&backup_dir)?;
    expect_storage_error(open(&store_dir)?.backup(&backup_dir));
    Ok(())
}

#[test]
fn test_backup_on_in_memory() -> Result<(), Box<dyn Error>> {
    let backup_dir = TempDir::new()?;
    expect_storage_error(Store::new()?.backup(&backup_dir));
    Ok(())
}

#[test]
fn test_durability_policies() -> Result<(), Box<dyn Error>> {
    for (name, durability) in [
        ("process", SlateDbDurability::Process),
        ("wal", SlateDbDurability::Wal),
        ("flush", SlateDbDurability::Flush),
    ] {
        let dir = TempDir::new()?;
        let store = Store::open_with_slate_db(
            SlateDbOptions::at_directory(dir.path()).with_durability(durability),
        )?;
        let quads = generated_quads(20, None);
        for quad in &quads {
            store.insert(quad.clone())?;
        }
        assert_eq!(store.len()?, quads.len(), "with the {name} policy");
        store.flush()?;
        store.validate()?;
        drop(store);
        let store = open(&dir)?;
        assert_eq!(
            store.len()?,
            quads.len(),
            "the {name} policy must not lose data once flushed"
        );
        store.validate()?;
    }
    Ok(())
}

#[test]
fn read_only_store_reads_without_fencing_the_writer() -> Result<(), Box<dyn Error>> {
    let dir = TempDir::new()?;
    let store = open(&dir)?;
    let quad = quads(GraphName::DefaultGraph)
        .into_iter()
        .next()
        .ok_or("the fixture has no quad")?;
    store.insert(quad.clone())?;

    let read_only = Store::open_slate_db_read_only(SlateDbOptions::at_directory(dir.path()))?;
    assert_eq!(read_only.len()?, 1, "a read-only store sees the data");
    assert!(read_only.contains(&quad)?);
    assert_eq!(
        read_only.iter().collect::<Result<Vec<_>, _>>()?,
        vec![quad.clone()]
    );

    // Every mutation is refused with a clear error instead of a SlateDB one.
    let error = expect_storage_error(read_only.insert(quad.clone()));
    assert!(
        error.to_string().contains("read-only"),
        "unexpected error: {error}"
    );

    // Opening read-only must not have fenced the writer of the same database.
    let other = Quad::new(
        NamedNode::new("http://example.com/s")?,
        NamedNode::new("http://example.com/p")?,
        NamedNode::new("http://example.com/o")?,
        GraphName::DefaultGraph,
    );
    store.insert(other)?;
    assert_eq!(store.len()?, 2);
    Ok(())
}

#[test]
fn file_url_object_store() -> Result<(), Box<dyn Error>> {
    let dir = TempDir::new()?;
    let url = format!("file://{}", dir.path().join("db").display());
    let store = Store::open_with_slate_db(SlateDbOptions::at_url(&url))?;
    for quad in generated_quads(10, None) {
        store.insert(quad)?;
    }
    assert_eq!(store.len()?, 10);
    store.validate()?;
    drop(store);

    let store = Store::open_with_slate_db(SlateDbOptions::at_url(&url))?;
    assert_eq!(store.len()?, 10, "the database survived the reopen");
    Ok(())
}

#[test]
fn an_unknown_object_store_scheme_is_refused() {
    let error = expect_storage_error(Store::open_with_slate_db(SlateDbOptions::at_url(
        "ftp://example.com/db",
    )));
    assert!(
        error.to_string().contains("not supported"),
        "unexpected error: {error}"
    );
}

/// Asserts that a database stored in an S3 bucket can be written and read back.
///
/// Ignored by default: it needs a running S3 compatible object store, like RustFS or MinIO, whose
/// URL is given in `OXIGRAPH_S3_TEST_URL` (`s3://bucket/oxigraph-test-<random>`), plus the usual
/// `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`, `AWS_ENDPOINT` and
/// `AWS_ALLOW_HTTP=true` variables. The objects it creates are left in the bucket.
#[test]
#[ignore = "needs a live S3 or RustFS endpoint in OXIGRAPH_S3_TEST_URL"]
fn s3_object_store_round_trip() -> Result<(), Box<dyn Error>> {
    let url = std::env::var("OXIGRAPH_S3_TEST_URL").map_err(|_| {
        "OXIGRAPH_S3_TEST_URL must hold the s3://bucket/prefix URL of the test database"
    })?;
    let store = Store::open_with_slate_db(SlateDbOptions::at_url(&url))?;
    let quads = generated_quads(100, None);
    store.extend(quads.clone())?;
    assert_eq!(store.len()?, quads.len());
    store.validate()?;
    store.clear()?;
    assert!(store.is_empty()?);

    let read_only = Store::open_slate_db_read_only(SlateDbOptions::at_url(&url))?;
    assert!(read_only.is_empty()?);
    Ok(())
}

/// Asserts that `result` failed and returns its error.
///
/// `Result::expect_err` would need the success type to implement `Debug`, which `()` does not.
fn expect_storage_error<T>(result: Result<T, StorageError>) -> StorageError {
    match result {
        Ok(_) => expected_failure(),
        Err(error) => error,
    }
}

#[expect(clippy::panic, reason = "the test helper above reports a missing failure")]
fn expected_failure() -> ! {
    panic!("the operation should have failed")
}

/// Peak resident memory of this process, in bytes, from `/proc/self/status`.
#[cfg(target_os = "linux")]
fn peak_rss_bytes() -> Result<u64, Box<dyn Error>> {
    memory_from_status("VmHWM:")
}

/// Resident memory of this process, in bytes, from `/proc/self/status`.
#[cfg(target_os = "linux")]
fn rss_bytes() -> Result<u64, Box<dyn Error>> {
    memory_from_status("VmRSS:")
}

#[cfg(target_os = "linux")]
fn memory_from_status(field: &str) -> Result<u64, Box<dyn Error>> {
    let status = std::fs::read_to_string("/proc/self/status")?;
    let value = status
        .lines()
        .find_map(|line| line.strip_prefix(field))
        .ok_or_else(|| format!("no {field} in /proc/self/status"))?;
    let kibibytes: u64 = value
        .trim()
        .trim_end_matches("kB")
        .trim()
        .parse::<u64>()?;
    Ok(kibibytes * 1024)
}

/// Runs `f` while sampling the resident memory every 10 ms.
///
/// Returns the memory before the run, the highest sample seen during it and how long it took.
#[cfg(target_os = "linux")]
fn peak_rss_while(
    f: impl FnOnce() -> Result<(), Box<dyn Error>>,
) -> Result<(u64, u64, Duration), Box<dyn Error>> {
    let baseline = rss_bytes()?;
    let peak = Arc::new(AtomicU64::new(baseline));
    let done = Arc::new(AtomicBool::new(false));
    let sampler = std::thread::spawn({
        let peak = Arc::clone(&peak);
        let done = Arc::clone(&done);
        move || {
            while !done.load(Ordering::Relaxed) {
                if let Ok(rss) = rss_bytes() {
                    peak.fetch_max(rss, Ordering::Relaxed);
                }
                std::thread::sleep(Duration::from_millis(10));
            }
        }
    });
    let started = Instant::now();
    let result = f();
    let elapsed = started.elapsed();
    done.store(true, Ordering::Relaxed);
    sampler
        .join()
        .map_err(|_| "the memory sampler panicked")?;
    result?;
    Ok((baseline, peak.load(Ordering::Relaxed), elapsed))
}

/// Measures the peak memory and the duration of a million quad bulk load, which must stay under
/// the `bulk_load_max_staged_bytes` budget instead of growing with the load. Ignored by default
/// because it takes a while; run it with
/// `cargo test --release -- --ignored --nocapture measure_million_quad_bulk_load_peak_memory`.
///
/// `OXIGRAPH_BENCH_QUADS` changes the number of quads and `OXIGRAPH_BULK_NON_ATOMIC` loads with
/// `BulkLoader::without_atomicity`, which is the documented way of loading a million quads.
#[test]
#[cfg(target_os = "linux")]
#[ignore = "loads a million quads, run it with --ignored to measure the peak memory"]
#[expect(clippy::print_stdout, clippy::use_debug, reason = "it reports its measurements")]
fn measure_million_quad_bulk_load_peak_memory() -> Result<(), Box<dyn Error>> {
    let count = std::env::var("OXIGRAPH_BENCH_QUADS")
        .ok()
        .and_then(|count| count.parse().ok())
        .unwrap_or(1_000_000);
    let non_atomic = std::env::var("OXIGRAPH_BULK_NON_ATOMIC").is_ok();
    let dir = TempDir::new()?;
    let store = open(&dir)?;
    let quads = generated_quads(count, None);
    let started = Instant::now();
    let mut loader = store.bulk_loader();
    if non_atomic {
        loader = loader.without_atomicity();
    }
    loader.load_quads(quads)?;
    let loaded = started.elapsed();
    println!("loading {count} quads took {loaded:?}");
    loader.commit()?;
    println!("commit took {:?}", started.elapsed().saturating_sub(loaded));
    assert_eq!(store.len()?, count);
    println!(
        "bulk load of {count} quads (non atomic: {non_atomic}): {:?}, peak RSS {} MiB",
        started.elapsed(),
        peak_rss_bytes()? / (1024 * 1024)
    );
    if count <= 100_000 {
        // `validate` checks every quad with point lookups, which is far too slow on a million of
        // them; it is covered by the other tests.
        store.validate()?;
    }
    Ok(())
}

/// Measures the memory of deleting a million quads, both quad by quad (`clear_graph`) and with the
/// delete range expansion of `clear`. Ignored by default because it takes a while; run it with
/// `cargo test --release -- --ignored --nocapture measure_million_quad_clear_peak_memory`.
///
/// `OXIGRAPH_BENCH_QUADS` changes the number of quads.
#[test]
#[cfg(target_os = "linux")]
#[ignore = "deletes a million quads, run it with --ignored to measure the peak memory"]
#[expect(clippy::print_stdout, clippy::use_debug, reason = "it reports its measurements")]
fn measure_million_quad_clear_peak_memory() -> Result<(), Box<dyn Error>> {
    let count = std::env::var("OXIGRAPH_BENCH_QUADS")
        .ok()
        .and_then(|count| count.parse().ok())
        .unwrap_or(1_000_000);
    let graph = GraphName::NamedNode(NamedNode::new_unchecked("http://example.com/g"));
    let dir = TempDir::new()?;
    let store = open(&dir)?;
    for (label, range_expansion) in [("clear_graph", false), ("clear", true)] {
        let mut loader = store.bulk_loader().without_atomicity();
        loader.load_quads(generated_quads(count, Some(&graph)))?;
        loader.commit()?;
        assert_eq!(store.len()?, count, "{label}: the load did not complete");
        let (baseline, peak, elapsed) = peak_rss_while(|| {
            if range_expansion {
                store.clear()?;
            } else {
                store.clear_graph(&graph)?;
            }
            Ok(())
        })?;
        println!(
            "{label} of {count} quads: {elapsed:?}, memory {} MiB before, {} MiB peak (+{} MiB)",
            baseline / (1024 * 1024),
            peak / (1024 * 1024),
            peak.saturating_sub(baseline) / (1024 * 1024)
        );
        assert!(store.is_empty()?, "{label}: the store is not empty");
    }
    Ok(())
}
