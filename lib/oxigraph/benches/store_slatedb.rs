//! Benchmarks of the SlateDB backend against the other persistent backends.
//!
//! The bodies live in `benches/common`, and every backend compiled in is measured, so that
//! `cargo bench --bench store_slatedb` produces the comparison table of
//! `planning/slatedb/performance.md` from a single dataset. Pass `OXIGRAPH_BENCH_QUADS` to change
//! the size of the load and deletion workloads.

mod common;

use codspeed_criterion_compat::{Criterion, criterion_group, criterion_main};

fn phase3(c: &mut Criterion) {
    common::insert_quads(c);
    common::extend_quads(c);
    common::bulk_load(c);
    common::point_lookup(c);
    common::predicate_scan(c);
    common::sparql_mix(c);
    common::delete_graph(c);
    common::cold_reopen(c);
}

criterion_group!(phase3_group, phase3);
criterion_main!(phase3_group);
