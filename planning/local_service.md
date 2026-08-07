I looked at the actual Oxigraph repository structure and crates rather than treating it as a generic SPARQL engine. Oxigraph is split into several crates: `oxrdf` (RDF model), `spargebra` (SPARQL parsing/algebra), `sparopt` (optimization), `spareval` (evaluation), `oxigraph` (store/database API), and the CLI/server layer. The repository explicitly supports SPARQL Federated Query, but the server implementation is a separate layer from the core database library. ([GitHub][1])

The modification should **not** add federation into `Store`, and it should **not** create a new planner. The existing SPARQL algebra already represents SERVICE. The correct modification is to introduce a local SERVICE execution path inside `spareval`, alongside the existing remote SERVICE implementation, with a URI resolver that maps a special URI scheme to an in-process `Store`. The final architecture should be:

```
spargebra
    |
    v
SPARQL algebra
    |
    v
spareval
    |
    +-----------------------------+
    |                             |
SERVICE <http://...>       SERVICE <oxigraph://...>
    |                             |
Remote SPARQL client        Local Store executor
    |                             |
HTTP                         Arc<Store>
                                  |
                                  v
                              RocksDB
```

The first code change is in the `spareval` crate, not the CLI. The SERVICE algebra operation should remain untouched because the query optimizer and evaluator already understand SERVICE as a graph pattern that produces bindings. The existing SERVICE execution path should be extracted behind an internal trait. Today the implementation path assumes that a SERVICE IRI is a remote SPARQL endpoint. Replace that assumption with a service resolver abstraction. Introduce an internal trait such as `ServiceHandler` or `ServiceExecutor` that accepts the SERVICE target IRI, the inner algebra pattern, the current variable bindings, and the evaluation context. The existing HTTP federation code becomes `HttpServiceExecutor`. Add `LocalStoreServiceExecutor` as the second implementation. This keeps the SPARQL semantics identical and only changes where the remote pattern is evaluated.

The second change is URI resolution. Do not use ordinary `file://` because SPARQL SERVICE IRIs conceptually identify query services, not RDF files. Add a new scheme such as `oxigraph://` or `store://`. The resolver should parse:

```
SERVICE <oxigraph:///data/fibo>
```

into:

```
LocalStoreServiceExecutor {
    path: "/data/fibo"
}
```

and:

```
SERVICE <http://localhost:7878/query>
```

into:

```
HttpServiceExecutor {
    endpoint: ...
}
```

This change belongs in the SERVICE dispatch code in `spareval`, not in the parser. The parser should continue treating the target as an IRI.

The third change is implementing the local executor. The local executor should not create another Oxigraph process. It should directly open the library API `Store` and execute against it. The implementation should look conceptually like:

```rust
struct LocalStoreServiceExecutor {
    store: Arc<Store>,
}
```

When the federation evaluator executes:

```
SERVICE <oxigraph:///data/fibo>
{
    ?x a fibo:Company
}
```

the executor should take the already prepared SERVICE sub-algebra and evaluate it using the same evaluator machinery used for a normal local query. The only difference is that the dataset being evaluated is not the parent query dataset but the selected child store.

The fourth change is adding a local store cache. A naïve implementation would do:

```
SERVICE oxigraph:///data/fibo
    |
    v
Store::open()
```

for every SERVICE invocation. That is wrong because RocksDB maintains resources, file locks, caches, and background state. Add a registry owned by the federation execution context:

```rust
struct LocalServiceRegistry {
    stores: DashMap<PathBuf, Arc<Store>>,
}
```

The first query opens the store. Subsequent SERVICE calls reuse the same `Arc<Store>`. This also allows multiple concurrent queries to share RocksDB state.

The fifth change is making the local executor use the existing federation binding mechanism. The most important part is not opening the database; it is preserving the SERVICE join semantics. The executor must accept incoming bindings exactly like the HTTP SERVICE executor. For example, after the first SERVICE produces:

```
?company = ex:Acme
```

the second SERVICE must effectively execute:

```sparql
VALUES ?company {
    ex:Acme
}

?company ex:ticker ?ticker
```

against the local store. Do not implement this as "run a complete independent query and join afterwards." The existing SERVICE execution flow already exists to support this. The local backend should only replace the transport.

The sixth change is adding tests at the `spareval` level. Create two temporary RocksDB stores during tests:

```
tests/
    data/
        fibo-store/
        yago-store/
```

Load small RDF datasets into them. Then execute:

```sparql
SELECT ?entity ?label
WHERE {
    SERVICE <oxigraph:///tmp/fibo-store> {
        ?entity a <http://example.org/Company>
    }

    SERVICE <oxigraph:///tmp/yago-store> {
        ?entity rdfs:label ?label
    }
}
```

The test should verify that results match the equivalent HTTP federation query. This proves that the planner, joins, and bindings remain unchanged.

The seventh change is adding lifecycle management. The CLI/server should not automatically expose this feature because the CLI normally owns one store location. Instead, the Rust library API should expose a federation configuration object. Something like:

```rust
FederationContext {
    local_services: LocalServiceRegistry,
    http_client: HttpClient,
}
```

The CLI could later add:

```
oxigraph serve --federated-config federation.toml
```

but that should not be part of the first implementation.

The eighth change is optional statistics integration. This is where local federation becomes better than HTTP federation. Since the stores are in-process, expose store metadata to the federation layer. Add:

```
LocalStoreServiceExecutor::statistics()
```

returning information such as:

```
triple count
predicate frequency
graph size
```

Then the optimizer can eventually make better SERVICE ordering decisions. This is not required for correctness because the current federation planner already works; it is a future optimization.

The final patch should therefore be relatively small. The required files are approximately:

```
spareval/
    src/
        eval/
            service.rs        <-- refactor SERVICE execution
            local_service.rs  <-- new
            http_service.rs   <-- existing logic moved here

oxigraph/
    src/
        store integration helpers

tests/
    service_local_store.rs
```

The key design decision is: **do not create "federated named graphs" and do not merge stores. Treat every Oxigraph database as a SERVICE endpoint, but allow SERVICE endpoints to be resolved to local `Store` instances instead of HTTP URLs.** This preserves SPARQL semantics, preserves the existing query planner, and adds only a new execution backend. ([GitHub][1])

[1]: https://github.com/oxigraph/oxigraph?utm_source=chatgpt.com "GitHub - oxigraph/oxigraph: SPARQL graph database · GitHub"

