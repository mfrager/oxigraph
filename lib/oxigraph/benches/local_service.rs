#![expect(clippy::panic)]

use bzip2::read::MultiBzDecoder;
use codspeed_criterion_compat::{Criterion, Throughput, criterion_group, criterion_main};
use oxhttp::model::{Request, Uri};
use oxigraph::io::{RdfFormat, RdfParser};
use oxigraph::model::vocab::{rdf, rdfs};
use oxigraph::model::{NamedNode, Quad};
use oxigraph::sparql::{LocalStoreServiceHandler, QueryResults, SparqlEvaluator};
use oxigraph::store::Store;
use spargebra::query::Query;
use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::str::FromStr;
use std::{fs, str};
use tempfile::TempDir;

// Graph IRIs used in the direct (named-graph) approach
const GRAPH_TYPES: &str = "http://oxigraph.org/graph/types";
const GRAPH_LABELS: &str = "http://oxigraph.org/graph/labels";

// === Per-size benchmark entry points ===

fn bench_n1000(c: &mut Criterion) {
    run_phase(c, 1_000);
}

fn bench_n5000(c: &mut Criterion) {
    run_phase(c, 5_000);
}

fn bench_n25000(c: &mut Criterion) {
    run_phase(c, 25_000);
}

fn bench_n100000(c: &mut Criterion) {
    run_phase(c, 100_000);
}

// === Core benchmark logic ===

fn run_phase(c: &mut Criterion, data_size: usize) {
    eprintln!("\n=== Phase: N={data_size} - downloading data ===");

    let raw_data = read_bz2_data(&format!(
        "https://zenodo.org/records/12663333/files/dataset-{data_size}.nt.bz2"
    ));

    eprintln!(
        "N={data_size}: {:.1} MB loaded, partitioning...",
        raw_data.len() as f64 / (1024.0 * 1024.0)
    );

    // Partition the NTriples data into 3 groups by predicate.
    // Group 0: rdf:type  → goes to named-graph or separate store 0
    // Group 1: rdfs:label, rdfs:comment → named-graph or separate store 1
    // Group 2: everything else → default graph or in-memory main store
    let (mut group0, mut group1, mut group2) = (Vec::new(), Vec::new(), Vec::new());
    for quad in RdfParser::from_format(RdfFormat::NTriples)
        .lenient()
        .for_slice(&raw_data)
    {
        let quad = quad.unwrap();
        if quad.predicate == rdf::TYPE {
            group0.push(quad);
        } else if quad.predicate == rdfs::LABEL || quad.predicate == rdfs::COMMENT {
            group1.push(quad);
        } else {
            group2.push(quad);
        }
    }

    eprintln!(
        "N={data_size}: partitioned → G0(types)={}, G1(labels)={}, G2(other)={}",
        group0.len(),
        group1.len(),
        group2.len()
    );

    // --- Build the DIRECT path: one on-disk store with 3 named graphs ---
    let direct_dir = TempDir::new().unwrap();
    let direct_path = direct_dir.path().to_string_lossy().to_string();
    {
        let store = Store::open(&direct_dir).unwrap();
        let mut loader = store.bulk_loader();

        let graph0 = NamedNode::new(GRAPH_TYPES).unwrap();
        let graph1 = NamedNode::new(GRAPH_LABELS).unwrap();

        loader
            .load_quads(group0.iter().map(|q| {
                Quad::new(
                    q.subject.clone(),
                    q.predicate.clone(),
                    q.object.clone(),
                    graph0.clone(),
                )
            }))
            .unwrap();
        loader
            .load_quads(group1.iter().map(|q| {
                Quad::new(
                    q.subject.clone(),
                    q.predicate.clone(),
                    q.object.clone(),
                    graph1.clone(),
                )
            }))
            .unwrap();
        loader.load_quads(group2.clone()).unwrap();
        loader.commit().unwrap();
        store.flush().unwrap();
        store.optimize().unwrap();
    }
    eprintln!("N={data_size}: DIRECT store ready at {direct_path}");

    // --- Build the SERVICE path: 2 on-disk stores + 1 in-memory store ---
    let svc0_dir = TempDir::new().unwrap();
    let svc0_path = svc0_dir.path().to_string_lossy().to_string();
    {
        let store = Store::open(&svc0_dir).unwrap();
        let mut loader = store.bulk_loader();
        loader.load_quads(group0.clone()).unwrap();
        loader.commit().unwrap();
        store.flush().unwrap();
        store.optimize().unwrap();
    }

    let svc1_dir = TempDir::new().unwrap();
    let svc1_path = svc1_dir.path().to_string_lossy().to_string();
    {
        let store = Store::open(&svc1_dir).unwrap();
        let mut loader = store.bulk_loader();
        loader.load_quads(group1.clone()).unwrap();
        loader.commit().unwrap();
        store.flush().unwrap();
        store.optimize().unwrap();
    }

    let main_store = Store::new().unwrap();
    {
        let mut loader = main_store.bulk_loader();
        loader.load_quads(group2.clone()).unwrap();
        loader.commit().unwrap();
    }

    eprintln!(
        "N={data_size}: SERVICE stores ready → svc0={svc0_path}, svc1={svc1_path}, main=memory"
    );

    // --- Build benchmark queries ---
    let (direct_queries, service_queries) = build_queries(&svc0_path, &svc1_path);

    eprintln!(
        "N={data_size}: {} query pairs built, verifying correctness...",
        direct_queries.len()
    );

    // --- Verify equivalence ---
    {
        let direct_store = Store::open_read_only(&direct_path).unwrap();
        let handler = LocalStoreServiceHandler::new();

        for (i, (dq, sq)) in direct_queries.iter().zip(&service_queries).enumerate() {
            let dc = count_results(
                &SparqlEvaluator::new(),
                &direct_store,
                dq,
            );
            let sc = count_results(
                &SparqlEvaluator::new().with_default_service_handler(handler.clone()),
                &main_store,
                sq,
            );
            if dc != sc {
                eprintln!(
                    "MISMATCH query {i}: direct={dc} service={sc}"
                );
                eprintln!("  Direct:  {}", dq.to_string().chars().take(300).collect::<String>());
                eprintln!("  Service: {}", sq.to_string().chars().take(300).collect::<String>());
            }
            assert_eq!(dc, sc, "Result mismatch for query {i} (N={data_size})");
        }
    }
    eprintln!("N={data_size}: correctness verified OK");

    // --- Benchmark ---
    let mut group = c.benchmark_group(format!("local_service N={data_size}"));
    group.sample_size(10);
    group.throughput(Throughput::Bytes(raw_data.len() as u64));

    {
        let direct_store = Store::open_read_only(&direct_path).unwrap();
        group.bench_function("direct (named graphs)", |b| {
            b.iter(|| run_queries(&direct_store, &direct_queries, None))
        });
    }

    {
        let handler = LocalStoreServiceHandler::new();
        group.bench_function("SERVICE (3 stores)", |b| {
            b.iter(|| run_queries(&main_store, &service_queries, Some(handler.clone())))
        });
    }

    group.finish();
    eprintln!("=== Phase: N={data_size} - complete ===\n");
}

// === Query construction ===
//
// We build equivalent query pairs for the two approaches.
//
// DIRECT:  uses GRAPH <iri> { ... } to target named graphs
// SERVICE: uses SERVICE <oxigraph:///path> { ... } to target separate stores
//
// Both join on shared variables across all 3 data sources.

fn build_queries(svc0_path: &str, svc1_path: &str) -> (Vec<Query>, Vec<Query>) {
    let bsbm = "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/";

    let query_specs: &[(&str, &str, &str)] = &[
        // Q1: Two-way join — types (store0) ▸ labels (store1)
        (
            "2-way: types + labels",
            &format!(
                "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                 PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                 SELECT ?s ?type ?label WHERE {{
                   GRAPH <{GRAPH_TYPES}>  {{ ?s rdf:type ?type . }}
                   GRAPH <{GRAPH_LABELS}> {{ ?s rdfs:label ?label . }}
                 }} LIMIT 50"
            ),
            &format!(
                "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                 PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                 SELECT ?s ?type ?label WHERE {{
                   SERVICE <oxigraph://{svc0_path}> {{ ?s rdf:type ?type . }}
                   SERVICE <oxigraph://{svc1_path}> {{ ?s rdfs:label ?label . }}
                 }} LIMIT 50"
            ),
        ),
        // Q2: Three-way join — types (store0) ▸ labels (store1) ▸ numeric (main)
        (
            "3-way: types + labels + numeric",
            &format!(
                "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                 PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                 PREFIX bsbm: <{bsbm}>
                 SELECT ?s ?type ?label ?value WHERE {{
                   GRAPH <{GRAPH_TYPES}>  {{ ?s rdf:type ?type . }}
                   GRAPH <{GRAPH_LABELS}> {{ ?s rdfs:label ?label . }}
                   ?s bsbm:productPropertyNumeric1 ?value .
                 }} LIMIT 50"
            ),
            &format!(
                "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                 PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                 PREFIX bsbm: <{bsbm}>
                 SELECT ?s ?type ?label ?value WHERE {{
                   SERVICE <oxigraph://{svc0_path}> {{ ?s rdf:type ?type . }}
                   SERVICE <oxigraph://{svc1_path}> {{ ?s rdfs:label ?label . }}
                   ?s bsbm:productPropertyNumeric1 ?value .
                 }} LIMIT 50"
            ),
        ),
        // Q3: Star join with FILTER — types + many numeric props from main
        (
            "star: types + filtered numeric",
            &format!(
                "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                 PREFIX bsbm: <{bsbm}>
                 SELECT ?s ?type ?v1 ?v2 WHERE {{
                   GRAPH <{GRAPH_TYPES}> {{ ?s rdf:type ?type . }}
                   ?s bsbm:productPropertyNumeric1 ?v1 .
                   ?s bsbm:productPropertyNumeric2 ?v2 .
                   FILTER(?v1 > 10 && ?v2 < 2000)
                 }} LIMIT 50"
            ),
            &format!(
                "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                 PREFIX bsbm: <{bsbm}>
                 SELECT ?s ?type ?v1 ?v2 WHERE {{
                   SERVICE <oxigraph://{svc0_path}> {{ ?s rdf:type ?type . }}
                   ?s bsbm:productPropertyNumeric1 ?v1 .
                   ?s bsbm:productPropertyNumeric2 ?v2 .
                   FILTER(?v1 > 10 && ?v2 < 2000)
                 }} LIMIT 50"
            ),
        ),
        // Q4: OPTIONAL across sources — types required, labels optional, numeric from main
        (
            "optional: types + optional labels + numeric",
            &format!(
                "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                 PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                 PREFIX bsbm: <{bsbm}>
                 SELECT ?s ?type ?label ?value WHERE {{
                   GRAPH <{GRAPH_TYPES}>  {{ ?s rdf:type ?type . }}
                   OPTIONAL {{ GRAPH <{GRAPH_LABELS}> {{ ?s rdfs:label ?label . }} }}
                   ?s bsbm:productPropertyNumeric1 ?value .
                 }} LIMIT 50"
            ),
            &format!(
                "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                 PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                 PREFIX bsbm: <{bsbm}>
                 SELECT ?s ?type ?label ?value WHERE {{
                   SERVICE <oxigraph://{svc0_path}> {{ ?s rdf:type ?type . }}
                   OPTIONAL {{ SERVICE <oxigraph://{svc1_path}> {{ ?s rdfs:label ?label . }} }}
                   ?s bsbm:productPropertyNumeric1 ?value .
                 }} LIMIT 50"
            ),
        ),
        // Q5: Wide star — all three sources, multiple properties from main
        (
            "wide star: types + labels + 3 numeric props",
            &format!(
                "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                 PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                 PREFIX bsbm: <{bsbm}>
                 SELECT ?s ?type ?label ?v1 ?v2 ?v3 WHERE {{
                   GRAPH <{GRAPH_TYPES}>  {{ ?s rdf:type ?type . }}
                   GRAPH <{GRAPH_LABELS}> {{ ?s rdfs:label ?label . }}
                   ?s bsbm:productPropertyNumeric1 ?v1 .
                   ?s bsbm:productPropertyNumeric2 ?v2 .
                   ?s bsbm:productPropertyNumeric3 ?v3 .
                 }} LIMIT 50"
            ),
            &format!(
                "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                 PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                 PREFIX bsbm: <{bsbm}>
                 SELECT ?s ?type ?label ?v1 ?v2 ?v3 WHERE {{
                   SERVICE <oxigraph://{svc0_path}> {{ ?s rdf:type ?type . }}
                   SERVICE <oxigraph://{svc1_path}> {{ ?s rdfs:label ?label . }}
                   ?s bsbm:productPropertyNumeric1 ?v1 .
                   ?s bsbm:productPropertyNumeric2 ?v2 .
                   ?s bsbm:productPropertyNumeric3 ?v3 .
                 }} LIMIT 50"
            ),
        ),
    ];

    let direct: Vec<Query> = query_specs
        .iter()
        .map(|(_, dq, _)| Query::from_str(dq).unwrap())
        .collect();
    let service: Vec<Query> = query_specs
        .iter()
        .map(|(_, _, sq)| Query::from_str(sq).unwrap())
        .collect();

    for ((name, _, _), (dq, sq)) in query_specs.iter().zip(direct.iter().zip(service.iter())) {
        eprintln!("  {name}: direct={} chars, service={} chars", dq.to_string().len(), sq.to_string().len());
    }

    (direct, service)
}

// === Query execution helpers ===

fn count_results(evaluator: &SparqlEvaluator, store: &Store, query: &Query) -> usize {
    match evaluator
        .clone()
        .for_query(query.clone())
        .on_store(store)
        .execute()
        .unwrap()
    {
        QueryResults::Boolean(b) => usize::from(b),
        QueryResults::Solutions(s) => s.count(),
        QueryResults::Graph(g) => g.count(),
    }
}

fn run_queries(
    store: &Store,
    queries: &[Query],
    handler: Option<LocalStoreServiceHandler>,
) {
    let evaluator = if let Some(h) = handler {
        SparqlEvaluator::new().with_default_service_handler(h)
    } else {
        SparqlEvaluator::new()
    };
    for query in queries {
        match evaluator
            .clone()
            .for_query(query.clone())
            .on_store(store)
            .execute()
            .unwrap()
        {
            QueryResults::Boolean(_) => {}
            QueryResults::Solutions(s) => {
                for solution in s {
                    solution.unwrap();
                }
            }
            QueryResults::Graph(g) => {
                for t in g {
                    t.unwrap();
                }
            }
        }
    }
}

// === Data loading utilities ===

fn read_bz2_data(url: &str) -> Vec<u8> {
    let url = Uri::from_str(url).unwrap();
    let target_data_dir = Path::new("benches").join("data");
    fs::create_dir_all(&target_data_dir).unwrap();
    let file_path = target_data_dir.join(url.path().split('/').next_back().unwrap());
    if !file_path.exists() {
        let client = oxhttp::Client::new()
            .with_redirection_limit(5)
            .with_user_agent(concat!("Oxigraph/", env!("CARGO_PKG_VERSION")))
            .unwrap();
        let request = Request::builder().uri(&url).body(()).unwrap();
        let response = client.request(request).unwrap();
        assert!(
            response.status().is_success(),
            "{url} returned {} with body:\n{}",
            response.status(),
            response.into_body().to_string().unwrap()
        );
        std::io::copy(
            &mut response.into_body(),
            &mut File::create(&file_path).unwrap(),
        )
        .unwrap();
    }
    let mut buf = Vec::new();
    MultiBzDecoder::new(File::open(&file_path).unwrap())
        .read_to_end(&mut buf)
        .unwrap();
    buf
}

// === Criterion registration ===

criterion_group!(n1000, bench_n1000);
criterion_group!(n5000, bench_n5000);
criterion_group!(n25000, bench_n25000);
criterion_group!(n100000, bench_n100000);
criterion_main!(n1000, n5000, n25000, n100000);
