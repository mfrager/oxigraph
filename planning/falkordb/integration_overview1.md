# FalkorDB vs. SlateDB: finding the better integration architecture

My initial assessment is that the proposed `falcon` architecture is unnecessarily high-level for the goal of making FalkorDB an Oxigraph storage backend.

There is a more fundamental distinction:

- SlateDB replaces the physical storage engine underneath Oxigraph while preserving RDF as the native storage model.
- FalkorDB is itself a graph execution and storage engine. Treating it merely as a Cypher target introduces an additional compiler and query-execution layer that may duplicate capabilities Oxigraph already has.

I would investigate a third architecture before committing to either design:

A low-level FalkorDB storage adapter that uses its native graph representation and execution facilities, but retains Oxigraph's RDF semantics, query algebra, and transaction coordination.

There is also a potentially important alternative: using FalkorDB's underlying graph engine more directly, rather than going through its Cypher interface.

The deciding question is whether that lower-level interface is sufficiently stable and complete to justify the additional integration work.

## 1. Important discovery: FalkorDB has changed substantially

The comparison appears to describe FalkorDB primarily through its older C-engine architecture.

As of November 12, 2026, the current FalkorDB 6.x codebase has a Rust implementation. Version 6.0.0 replaced the previous C engine, while retaining the Redis module interface, `GRAPH.*` commands, and Cypher compatibility. The latest release I found is 6.0.4.

This materially changes the integration decision.

FalkorDB 6 architecture

Rust engine

Redis module / command layer

GRAPH.QUERY · GRAPH.BULK · GRAPH.RO_QUERY

FalkorDB Rust graph crate

Graph model · Cypher planner · Runtime · GraphBLAS

GraphBLAS

Sparse matrices and traversal

Versioned graph

MVCC and copy-on-write

The current repository separates the graph implementation into a `graph` Rust workspace crate, while the root crate handles Redis module integration. The graph crate contains the graph structures, query processing, and runtime.

This raises a much better possibility:

Integrate the graph crate into Oxigraph directly, without going through Redis, Cypher, or the FalkorDB client.

That could eliminate:

- Redis protocol overhead.
- Cypher generation and parsing.
- Cypher result serialization and decoding.
- The intermediate property-graph mapping layer.
- An extra server process.
- Some unnecessary memory copies.

It also potentially gives Oxigraph direct access to FalkorDB's graph MVCC and GraphBLAS data structures.

There is a catch: the internal graph crate is not automatically a supported public embedding API. We need to distinguish what is technically accessible from what is stable and maintainable.

## 2. What the actual FalkorDB code exposes

I examined the current Rust graph crate, including:

- `graph/src/graph/graph.rs`
- `graph/src/graph/mvcc_graph.rs`
- `graph/src/graph/graphblas/`
- `graph/src/storage/`
- `src/graph_core.rs`
- `src/lib.rs`

The most interesting discovery is that FalkorDB's graph engine already has almost exactly the kind of primitives an embedded Oxigraph backend would need.

### 2.1 Native graph representation

The current `Graph` implementation uses:

| Structure          | Implementation                        |
| ------------------ | ------------------------------------- |
| Node identity      | 64-bit IDs                            |
| Overall adjacency  | GraphBLAS boolean sparse matrix       |
| Node labels        | Per-label diagonal sparse matrices    |
| Relationship types | Per-type sparse 3D tensors            |
| Edge identity      | Independent 64-bit relationship IDs   |
| Edge endpoints     | Reverse endpoint index                |
| Properties         | Block-allocated attribute stores      |
| Versioning         | Copy-on-write matrices and attributes |

The relationship tensor is particularly interesting:

\\[ T_p(s,o,e) \\]

Where:

- \\(p\\) is the relationship type.
- \\(s\\) is the source node.
- \\(o\\) is the destination node.
- \\(e\\) is the unique edge ID.

It supports multiple relationships between the same pair of nodes.

The overall adjacency matrix provides fast untyped connectivity, while the per-type tensors provide typed graph traversal.

This maps surprisingly well to RDF.

### 2.2 The RDF mapping becomes much simpler

Instead of creating an elaborate property-graph schema, use FalkorDB's existing graph primitives directly.

Proposed RDF-to-FalkorDB mapping

RDF terms → FalkorDB nodes

IRI

Resource node

Blank node

Resource node

Literal

Literal node

RDF predicates → relationship types

Each predicate maps to an internal relationship type ID.

## Alice ──knows──→ Bob

Relationship type = predicate ID

Named graphs → edge metadata

Store graph_id on each relationship, with a reserved value for the default graph.

A quad becomes:

\\[ (s,p,o,g)\\rightarrow (\\operatorname{node}(s),\\operatorname{type}(p), \\operatorname{node}(o),g) \\]

Use FalkorDB's native relationship IDs to distinguish individual quads.

Maintain a separate uniqueness mapping:

```
(s_id, p_id, o_id, g_id) → edge_id
```

This is necessary because RDF datasets have set semantics, whereas FalkorDB permits multiple edges with identical endpoints and relationship types.

The important advantage is that there is no need to create:

- An RDF quad node.
- Four connecting relationships.
- A separate predicate node.
- A separate graph node for every quad.

The native graph representation already supplies the necessary relationship identity.

### 2.3 Use bulk graph operations directly

FalkorDB 6 exposes a particularly useful method:

```
Graph::create_relationships_bulk(
    &mut self,
    type_name: &Arc<String>,
    srcs: &[u64],
    dsts: &[u64],
    rel_ids: &[u64],
)
```

It creates relationships using flat arrays and updates the relationship tensors, endpoint index, overall adjacency matrix, and relationship-type matrix.

This avoids generating Cypher and passing through the query planner and runtime.

For RDF imports, this is a much better starting point than `UNWIND ... CREATE`.

There is one optimization I would make: resolve predicate type IDs once, cache them in the adapter, and avoid repeatedly looking up predicate names. The current graph implementation has a linear search in its relationship-type lookup path.

## 3. FalkorDB already implements MVCC

This is another major discovery.

The current `MvccGraph` implementation uses `ArcSwap` and copy-on-write graph versions.

Its model is:

Committed graph version

Graph V1

Published immutable snapshot

fork()

Reader A

V1

Retains snapshot

Writer

V2

Private COW version

commit(V2)

Graph V2

New published version

Existing readers retain V1 until their snapshot references are released.

The actual implementation provides:

- `MvccGraph::read()`
- `MvccGraph::fork()`
- `MvccGraph::is_stale()`
- `MvccGraph::commit()`

The graph is forked using shallow copies of copy-on-write structures. A successful commit publishes the new version through an atomic pointer swap.

This is much closer to Oxigraph's desired transaction behavior than the earlier comparison suggests.

The important qualification: FalkorDB's MVCC provides in-memory snapshot isolation, not durable transaction commits. Its host must also coordinate write publication. Oxigraph would need to supply that coordination and integrate persistence into the commit protocol.

### Implication for Oxigraph

Do not implement a second MVCC layer on top of FalkorDB.

Reuse its graph-versioning machinery and adapt the Oxigraph transaction interface to it.

There is no reason to maintain two independent COW graph snapshots if FalkorDB already supplies them.

## 4. The significant limitation: persistence is not abstracted

This is where the direct integration becomes more complicated.

The current graph crate has a `storage` module with two storage interfaces:

- `IndexBackend`
- `AttrBackend`

However, these are narrowly scoped storage extension points. They are not an abstraction over the GraphBLAS adjacency matrices, relationship tensors, or complete graph state. The traits are also currently minimal stubs.

The underlying matrices and attribute structures are still memory-resident.

FalkorDB's graph serialization is also integrated with its Redis module's RDB persistence machinery.

The graph crate exposes useful pieces such as `Graph::encode_payload()` and `Graph::restore()`, but these are not a complete, independently supported durable storage interface.

Therefore:

Directly embedding FalkorDB's graph crate solves the graph execution problem, but does not solve the durable storage problem.

You would need to implement:

- Graph checkpoint serialization.
- Incremental transaction persistence.
- Recovery and replay.
- Durable commit ordering.
- Snapshot retention and reclamation.

This is entirely achievable, but it needs to be part of the architecture rather than an afterthought.

## 5. Two possible integration architectures

Now that we know what the code actually exposes, we can compare the approaches.

### A. Cypher compiler + FalkorDB server

Not recommended

Oxigraph / SPARQL

RDF → property graph compiler

Cypher generation and parsing

Redis protocol → FalkorDB

Problems

- Two query planners.
- Unnecessary compilation.
- Network or process boundary.
- Cypher translation and semantic compatibility.
- Duplicate transaction coordination.

### B. Direct FalkorDB graph crate with native persistence

Recommended

Oxigraph / SPARQL

Native RDF storage adapter

FalkorDB Graph + MvccGraph

GraphBLAS matrices and tensors

FalkorDB Persistence Layer (custom)

Durability · WAL · Checkpoints · Recovery

Advantages

- No Redis.
- No Cypher.
- No serialization across a protocol boundary.
- Direct GraphBLAS access.
- Existing COW and MVCC.
- Native bulk graph operations.
- Single unified persistence model.
- No external dependency for durability.

This is the correct direction if the goal is to make FalkorDB the standalone authoritative backend for Oxigraph.

## 6. Recommended low-level implementation

I would structure the integration as a new `oxigraph-falkor` backend, with a deliberately narrow interface between Oxigraph and FalkorDB.

### 6.1 Storage layout

```
oxigraph-falkor/
  src/
    lib.rs
    backend.rs
    graph.rs
    terms.rs
    quads.rs
    patterns.rs
    transactions.rs
    snapshots.rs
    persistence.rs
    recovery.rs
    bulk.rs
    indexes.rs
```

The graph engine should be accessed directly:

```
use graph::graph::{
    graph::Graph,
    mvcc_graph::MvccGraph,
};
```

The actual package would initially be a pinned Git dependency or vendored fork of FalkorDB's `graph` workspace crate.

Do not depend on the Redis-facing root crate for this architecture.

### 6.2 RDF identity layer

Use Oxigraph's existing term encoding as the canonical representation.

Maintain two mappings:

```
RDF Term ID ↔ FalkorDB Node ID
```

And:

```
RDF Quad ID ↔ FalkorDB Relationship ID
```

A compact term dictionary can use:

```
struct TermEntry {
    term_id: u128,
    graph_node_id: u64,
}
```

The actual implementation should preserve Oxigraph's encoded terms, including its existing hash and dictionary collision handling.

The predicate mapping becomes:

```
predicate_id → FalkorDB TypeId
```

Use a dedicated internal predicate registry rather than repeatedly searching FalkorDB's type-name vector.

### 6.3 Relationship representation

I would benchmark two alternatives:

| Mapping               | Description                                             |
| --------------------- | ------------------------------------------------------- |
| Typed relationships   | One relationship type per RDF predicate                 |
| Generic relationships | One `RDF_EDGE` type, with predicate ID as edge metadata |

I favor typed relationships because FalkorDB already maintains an independent sparse tensor for each relationship type.

For the generic approach, every predicate lookup would need to filter edge metadata rather than directly selecting its relationship tensor.

The typed approach is likely better for conventional RDF workloads with a reasonably bounded predicate vocabulary.

Named graph identity remains an edge attribute:

```
graph_id: u64
```

For graph-specific queries, add an appropriate graph membership index rather than scanning every edge of a predicate.

### 6.4 Query execution

Keep Oxigraph's existing SPARQL engine.

The backend implements the existing quad-pattern access interface, using FalkorDB's matrices and tensors.

For example:

```
SELECT ?friend
WHERE {
    <alice> <knows> ?friend
}
```

Becomes a direct lookup of the predicate's relationship tensor and the subject's outgoing adjacency.

Conceptually:

```
fn scan_sp(
    graph: &Graph,
    subject: NodeId,
    predicate: TypeId,
) -> impl Iterator<Item = NodeId> {
    // Enumerate outgoing edges of the predicate tensor.
}
```

The actual implementation should use the existing matrix/tensor iterators and avoid materializing intermediate collections.

For other access patterns:

| SPARQL pattern | Graph engine operation                  |
| -------------- | --------------------------------------- |
| S, P, O        | Direct edge identity check              |
| S, P, ?O       | Outgoing typed adjacency                |
| ?S, P, O       | Incoming typed adjacency                |
| ?S, P, ?O      | Predicate tensor iteration              |
| S, ?P, O       | Adjacency plus relationship type lookup |
| ?S, ?P, O      | Incoming adjacency and type resolution  |
| S, ?P, ?O      | Overall adjacency                       |
| ?S, ?P, ?O     | Full graph scan                         |

Named graph restrictions are applied using the graph metadata.

This provides the physical access layer without creating a second query language.

Important optimization: Oxigraph's SPARQL planner should continue to estimate selectivity and choose join order. FalkorDB should provide efficient access primitives, not dictate SPARQL execution.

## 7. Persistence: native FalkorDB durability layer

Since we are not using SlateDB, we must implement a native persistence layer for FalkorDB's graph state. This layer sits alongside the graph engine and provides:

- Write-ahead logging (WAL) for transaction durability
- Periodic graph checkpoints (serialized GraphBLAS matrices, tensors, attributes)
- Crash recovery with replay
- Snapshot management and reclamation

### 7.1 Persistence architecture

FalkorDB graph state
Graph V42
GraphBLAS matrices · Relationship tensors · Attributes
Checkpoint + transaction log
Versioned graph snapshot · Incremental operations · Checksums
FalkorDB Native Persistence
WAL · Checkpoints · Recovery · Snapshot metadata

### 7.2 Checkpoint format

Define an independent, versioned checkpoint format (not Redis RDB):

```
struct GraphCheckpoint {
    version: u64,
    transaction_version: u64,
    node_count: u64,
    edge_count: u64,
    adjacency_matrix: SerializedMatrix,
    label_matrices: Vec<(u64, SerializedMatrix)>,
    relationship_tensors: Vec<(u64, SerializedTensor)>,
    attribute_stores: SerializedAttributes,
    predicate_registry: Vec<(u64, u64)>,
    term_mappings: Vec<(u128, u64)>,
    checksum: u64,
}
```

FalkorDB's existing `Graph::encode_payload()` provides a useful starting point, but we introduce our own format to avoid depending on Redis RDB serialization.

### 7.3 Incremental transaction log

Persist graph mutations as compact operations:

```
enum GraphOperation {
    CreateNodes(Vec<u64>),
    CreateEdges {
        predicate_type_id: u64,
        subjects: Vec<u64>,
        objects: Vec<u64>,
        edge_ids: Vec<u64>,
        graph_ids: Vec<u64>,
    },
    DeleteEdges(Vec<u64>),
    UpdateAttributes(Vec<AttributeUpdate>),
    CreatePredicateType { predicate_id: u64, type_id: u64 },
    CreateTermMapping { term_id: u128, node_id: u64 },
}
```

The log has monotonically increasing transaction versions, checksums, and idempotent replay semantics.

### 7.4 Atomic commit sequence

This is essential:

1. Pin the current committed graph snapshot.
2. Fork the FalkorDB graph using COW.
3. Apply mutations to the private graph version.
4. Prepare corresponding RDF dictionary, index, and graph metadata changes.
5. **Write transaction log entry to WAL (durable - linearization point).**
6. Publish the new FalkorDB graph version (atomic pointer swap).
7. Release the previous version when readers finish.
8. Periodically checkpoint (async, non-blocking).

The WAL write is the durability boundary. If the process crashes after WAL write but before graph publication, recovery replays the WAL. If WAL write fails, discard the private graph version.

A published graph version carries its persistent transaction version so readers always access matching graph and dictionary snapshots.

### 7.5 Recovery protocol

On startup:

1. Load latest checkpoint (if any).
2. Replay WAL from checkpoint transaction version forward.
3. Reconstruct graph state in memory.
4. Verify checksums.
5. Publish recovered graph version.

## 8. What about using FalkorDB's native storage interfaces?

I would not try to use the existing `IndexBackend` and `AttrBackend` interfaces as they currently stand.

They are not yet a complete storage abstraction.

Instead, I would extend the graph engine with a higher-level persistence contract:

```
trait GraphPersistence: Send + Sync {
    type Snapshot;
    type Transaction;

    fn load_snapshot(&self) -> Result<Self::Snapshot>;
    fn prepare(&self, version: u64, operations: &[GraphOperation]) -> Result<Self::Transaction>;
    fn commit(&self, transaction: Self::Transaction) -> Result<()>;
    fn recover(&self, snapshot: &Self::Snapshot) -> Result<()>;
}
```

This is illustrative; the actual interface needs to support asynchronous persistence and durability modes.

I would also introduce a checkpoint interface and explicit snapshot reclamation.

The important design principle is to keep GraphBLAS's in-memory execution representation separate from durable storage representation.

Do not attempt to make individual GraphBLAS matrix operations behave like key-value operations.

That would undermine the performance characteristics of both systems.

## 9. Important performance considerations

There are four areas I would benchmark before committing to the design.

| Issue                 | Potential problem                       | Mitigation                                     |
| --------------------- | --------------------------------------- | ---------------------------------------------- |
| Predicate cardinality | One tensor per predicate                | Compare typed and generic edge representations |
| Named graphs          | Graph filtering adds edge metadata work | Graph-aware indexes                            |
| Term dictionary       | Additional ID translation               | Cache bidirectional mappings                   |
| COW snapshots         | Writes can trigger matrix copies        | Measure write amplification and batch updates  |

There is another subtle issue: FalkorDB's graph representation has a global node ID space.

Oxigraph's RDF term IDs and FalkorDB's graph node IDs should not be assumed interchangeable. Use a persistent mapping, and preserve the ability to reconstruct it.

Also, FalkorDB's overall adjacency matrix represents connectivity without preserving predicate identity. Predicate-aware operations must use the relationship tensors, not the global adjacency matrix alone.

### Benchmark the actual advantage

I would use the following test matrix:

1. 10 million triples, 100 predicates.
2. 10 million triples, 10,000 predicates.
3. 100 million triples, highly skewed predicates.
4. Many named graphs.
5. High-degree nodes.
6. Highly selective and nonselective SPARQL joins.
7. Concurrent readers and sustained writes.
8. Snapshot creation and recovery.

Compare:

- Existing RocksDB backend.
- Existing SlateDB backend.
- FalkorDB direct graph crate with native persistence.

Measure RAM consumption particularly carefully. FalkorDB is an in-memory graph engine; its sparse matrices still require resident memory. It does not inherit object-storage elasticity.

## 10. Important licensing and dependency issue

There is one potentially decisive blocker.

The FalkorDB graph crate's own `Cargo.toml` specifies `SSPL-1.0`. The current official Rust client is MIT-licensed, but its embedded mode bundles the SSPL-licensed server module.

That means:

| Integration                         | Licensing consideration                  |
| ----------------------------------- | ---------------------------------------- |
| External FalkorDB server            | SSPL server obligations                  |
| Embedded Redis + FalkorDB module    | SSPL module obligations                  |
| Direct FalkorDB graph crate         | SSPL graph engine obligations            |
| Standalone GraphBLAS implementation | Separate dependency and licensing review |

I would obtain a licensing review before incorporating the graph crate into an Apache/MIT-distributed Oxigraph product.

The direct Rust integration is technically attractive, but its licensing is not interchangeable with the official MIT Rust client.

## 11. Revised implementation roadmap

I would replace the original compiler-centric `falcon` plan with this sequence.

1. Phase 1 — FalkorDB graph-core proof of concept
   - Pin FalkorDB 6.0.4.
   - Build the `graph` crate independently.
   - Create RDF resource and literal nodes.
   - Map predicates to relationship types.
   - Use `create_nodes()` and `create_relationships_bulk()`.
   - Test outgoing and incoming traversal.
   - Verify duplicate quad handling.
   Success criterion: Native graph operations without Redis or Cypher.

2. Phase 2 — Oxigraph adapter
   - Implement the storage reader/writer interface.
   - Implement all quad-pattern access paths.
   - Integrate `MvccGraph`.
   - Preserve named graph and RDF term semantics.
   - Run the existing SPARQL conformance tests.
   Success criterion: Oxigraph queries execute directly against FalkorDB graph structures.

3. Phase 3 — Native FalkorDB persistence
   - Implement WAL for transaction durability.
   - Implement graph checkpoint serialization.
   - Implement crash recovery with WAL replay.
   - Coordinate WAL commit and graph publication.
   - Add durability configuration.
   Success criterion: Restart produces the exact last committed RDF dataset.

4. Phase 4 — Graph optimization
   - Optimize predicate tensors.
   - Add named-graph indexes.
   - Optimize bound and unbound pattern scans.
   - Add GraphBLAS-based multi-hop optimizations.
   - Benchmark join execution.
   Success criterion: Demonstrable performance advantages on graph-oriented SPARQL workloads.

5. Phase 5 — Advanced storage capabilities
   - Snapshot retention.
   - Branches and copy-on-write generations.
   - Incremental replication.
   - Bounded checkpointing.
   - Recovery verification.
   - Optional BFT transaction ordering.
   Success criterion: Unified versioned RDF and graph execution architecture.

## 12. Final decision

Recommended direction

I would make a significant change to the original proposal.

Do not build a SPARQL-to-Cypher compiler to integrate FalkorDB into Oxigraph.

Instead, directly integrate FalkorDB's Rust graph core.

The best architecture is:

## Oxigraph + FalkorDB (native persistence)

Oxigraph

RDF semantics · SPARQL · Query planning

Native RDF storage adapter

Term mapping · Quad access · Transactions

FalkorDB graph core

- GraphBLAS
- Sparse tensors
- COW
- MVCC
- Traversal

FalkorDB Native Persistence

- Term dictionary
- WAL
- Checkpoints
- Snapshots
- Recovery

The resulting division of responsibilities is clean:

| Component           | Responsibility                                    |
| ------------------- | ------------------------------------------------- |
| Oxigraph            | RDF semantics and SPARQL                          |
| FalkorDB graph core | Graph representation and execution acceleration   |
| FalkorDB Persistence | Durable storage and recovery                     |
| Adapter             | RDF-to-graph mapping and transaction coordination |

There is one strategic distinction worth preserving:

If the goal is to use FalkorDB's complete standalone database, including its own Redis persistence and independent server lifecycle, keep it as a separate service and use Cypher.

If the goal is to build the fastest possible Oxigraph backend using FalkorDB's underlying technology, embed its graph core and bypass the Redis/Cypher layers.

I would pursue the second approach, subject to resolving SSPL licensing. It eliminates the largest amount of unnecessary machinery while reusing FalkorDB's most valuable technology: its GraphBLAS-based graph representation and COW MVCC implementation.

One further opportunity: this architecture would make it possible to have the same RDF dataset support conventional SPARQL, graph traversal, graph algorithms, and eventually your broader ontology/semantic execution system without compiling everything into Cypher.

### Primary source code and documentation

- [FalkorDB graph engine source](https://github.com/FalkorDB/FalkorDB/tree/main/graph/src/graph?utm_source=chatgpt.com)
- [Graph implementation](https://github.com/FalkorDB/FalkorDB/blob/main/graph/src/graph/graph.rs?utm_source=chatgpt.com)
- [MVCC implementation](https://github.com/FalkorDB/FalkorDB/blob/main/graph/src/graph/mvcc_graph.rs?utm_source=chatgpt.com)
- [GraphBLAS implementation](https://github.com/FalkorDB/FalkorDB/blob/main/graph/src/graph/graphblas?utm_source=chatgpt.com)
- [Storage abstraction](https://github.com/FalkorDB/FalkorDB/blob/main/graph/src/storage?utm_source=chatgpt.com)
- [Oxigraph storage architecture](https://github.com/oxigraph/oxigraph/wiki/Architecture?utm_source=chatgpt.com)

The next useful engineering artifact would be a concrete Rust-level integration specification for `oxigraph-falkor`, including the exact Graph APIs to reuse, the minimal fork changes, and the native persistence commit/recovery protocol.