//! Tests for local SERVICE federation via `LocalStoreServiceHandler`.
//!
//! Verifies that `SERVICE <oxigraph://...>` URIs resolve to local RocksDB
//! stores and return the same results as an equivalent query against the
//! same data loaded into an in-memory store.

#![cfg(test)]
#![expect(clippy::panic_in_result_fn)]

#[cfg(all(not(target_family = "wasm"), feature = "rocksdb"))]
mod test {
    use oxigraph::io::RdfFormat;
    use oxigraph::model::*;
    use oxigraph::sparql::{LocalStoreServiceHandler, QueryResults, SparqlEvaluator};
    use oxigraph::store::Store;
    use std::error::Error;
    use tempfile::TempDir;

    const COMPANIES: &str = "\
@prefix ex: <http://example.org/> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .

ex:Acme a ex:Company ;
    rdfs:label \"Acme Corp\" ;
    ex:ticker \"ACME\" .

ex:Globex a ex:Company ;
    rdfs:label \"Globex Corporation\" ;
    ex:ticker \"GLBX\" .
";

    const EMPLOYEES: &str = "\
@prefix ex: <http://example.org/> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .

ex:Acme ex:employeeCount 1500 .

ex:Globex ex:employeeCount 3200 .
";

    /// Create a temporary on-disk store populated with the given Turtle data.
    /// Returns the TempDir (keeping it alive) and the Store handle.
    /// The caller should drop the Store before querying to release the write lock.
    fn populated_store(data: &str) -> Result<(TempDir, Store), Box<dyn Error>> {
        let dir = TempDir::new()?;
        let store = Store::open(&dir)?;
        store.load_from_reader(RdfFormat::Turtle, data.as_bytes())?;
        store.flush()?;
        Ok((dir, store))
    }

    #[test]
    fn local_service_single_store() -> Result<(), Box<dyn Error>> {
        let (dir, store) = populated_store(COMPANIES)?;
        let dir_path = dir.path().to_string_lossy().to_string();
        drop(store); // release the write lock before querying

        let handler = LocalStoreServiceHandler::new();
        let query = format!(
            "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            SELECT ?label WHERE {{ SERVICE <oxigraph://{}> {{ ?company rdfs:label ?label }} }}",
            dir_path
        );

        let evaluator = SparqlEvaluator::new().with_default_service_handler(handler);
        if let QueryResults::Solutions(solutions) = evaluator
            .parse_query(&query)?
            .on_store(&Store::new()?)
            .execute()?
        {
            let results: Vec<_> = solutions
                .map(|r| {
                    let r = r?;
                    Ok(r.get("label").unwrap().to_string())
                })
                .collect::<Result<Vec<_>, Box<dyn Error>>>()?;
            assert_eq!(results.len(), 2);
            assert!(results.contains(&"\"Acme Corp\"".to_string()));
            assert!(results.contains(&"\"Globex Corporation\"".to_string()));
        } else {
            panic!("Expected solutions");
        }
        Ok(())
    }

    #[test]
    fn local_service_federated_join() -> Result<(), Box<dyn Error>> {
        let (comp_dir, comp_store) = populated_store(COMPANIES)?;
        let (emp_dir, emp_store) = populated_store(EMPLOYEES)?;
        let comp_path = comp_dir.path().to_string_lossy().to_string();
        let emp_path = emp_dir.path().to_string_lossy().to_string();
        drop(comp_store);
        drop(emp_store);

        let handler = LocalStoreServiceHandler::new();
        let query = format!(
            "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            SELECT ?label ?count WHERE {{
                SERVICE <oxigraph://{comp_path}> {{
                    ?company a <http://example.org/Company> ;
                             rdfs:label ?label .
                }}
                SERVICE <oxigraph://{emp_path}> {{
                    ?company <http://example.org/employeeCount> ?count .
                }}
            }}",
            comp_path = comp_path,
            emp_path = emp_path
        );

        let evaluator = SparqlEvaluator::new().with_default_service_handler(handler);
        if let QueryResults::Solutions(solutions) = evaluator
            .parse_query(&query)?
            .on_store(&Store::new()?)
            .execute()?
        {
            let results: Vec<_> = solutions
                .map(|r| {
                    let r = r?;
                    Ok((
                        r.get("label").unwrap().to_string(),
                        r.get("count").unwrap().to_string(),
                    ))
                })
                .collect::<Result<Vec<_>, Box<dyn Error>>>()?;
            assert_eq!(results.len(), 2);
            assert!(results.contains(&(
                "\"Acme Corp\"".to_string(),
                "\"1500\"^^<http://www.w3.org/2001/XMLSchema#integer>".to_string()
            )));
            assert!(results.contains(&(
                "\"Globex Corporation\"".to_string(),
                "\"3200\"^^<http://www.w3.org/2001/XMLSchema#integer>".to_string()
            )));
        } else {
            panic!("Expected solutions");
        }
        Ok(())
    }

    #[test]
    fn local_service_silent_on_missing_store() -> Result<(), Box<dyn Error>> {
        let handler = LocalStoreServiceHandler::new();

        // SERVICE SILENT on non-existent path should not error.
        // When SERVICE fails with SILENT, the input bindings are preserved.
        // Since the query has no outer pattern, we get one empty solution.
        let evaluator = SparqlEvaluator::new().with_default_service_handler(handler);
        if let QueryResults::Solutions(mut solutions) = evaluator
            .parse_query(
                "SELECT * WHERE { SERVICE SILENT <oxigraph:///nonexistent/path> { ?s ?p ?o } }",
            )?
            .on_store(&Store::new()?)
            .execute()?
        {
            // One empty solution is returned (the input binding preserved by SILENT)
            assert!(solutions.next().is_some());
            assert!(solutions.next().is_none());
        }
        Ok(())
    }

    #[test]
    fn local_service_unsupported_scheme_is_error() -> Result<(), Box<dyn Error>> {
        let handler = LocalStoreServiceHandler::new();

        // Non-oxigraph:// schemes should produce an error inside the solutions
        // iterator (the overall execute() succeeds but the solution stream
        // carries the error).
        let evaluator = SparqlEvaluator::new().with_default_service_handler(handler);
        if let QueryResults::Solutions(mut solutions) = evaluator
            .parse_query("SELECT * WHERE { SERVICE <http://example.com/sparql> { ?s ?p ?o } }")?
            .on_store(&Store::new()?)
            .execute()?
        {
            let first = solutions.next().unwrap();
            assert!(first.is_err());
        }
        Ok(())
    }

    #[test]
    fn local_service_handler_is_cloneable() -> Result<(), Box<dyn Error>> {
        let handler = LocalStoreServiceHandler::new();
        let _cloned = handler.clone();
        Ok(())
    }

    #[test]
    fn local_service_default_constructor() -> Result<(), Box<dyn Error>> {
        let _handler = LocalStoreServiceHandler::default();
        Ok(())
    }

    // ── Fused SERVICE execution tests ──────────────────────────────────

    /// Verifies that the fused path (`with_local_service_handler`) returns exactly
    /// the same results as the handler path (`with_default_service_handler`).
    #[test]
    fn local_service_fused_matches_handler() -> Result<(), Box<dyn Error>> {
        let (dir, store) = populated_store(COMPANIES)?;
        let dir_path = dir.path().to_string_lossy().to_string();
        drop(store);

        let query = format!(
            "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            SELECT ?label WHERE {{ SERVICE <oxigraph://{dir_path}> {{ ?company rdfs:label ?label }} }}"
        );

        let evaluate = |fused: bool| -> Result<Vec<String>, Box<dyn Error>> {
            let evaluator = if fused {
                SparqlEvaluator::new().with_local_service_handler(LocalStoreServiceHandler::new())
            } else {
                SparqlEvaluator::new()
                    .with_default_service_handler(LocalStoreServiceHandler::new())
            };
            if let QueryResults::Solutions(solutions) = evaluator
                .parse_query(&query)?
                .on_store(&Store::new()?)
                .execute()?
            {
                solutions
                    .map(|r| {
                        let r = r?;
                        Ok(r.get("label").unwrap().to_string())
                    })
                    .collect::<Result<Vec<_>, Box<dyn Error>>>()
            } else {
                panic!("Expected solutions")
            }
        };

        let handler_results = evaluate(false)?;
        let fused_results = evaluate(true)?;
        assert_eq!(fused_results, handler_results);
        assert_eq!(fused_results.len(), 2);
        Ok(())
    }

    /// Fused path: federated join between two local stores.
    #[test]
    fn local_service_fused_federated_join() -> Result<(), Box<dyn Error>> {
        let (comp_dir, comp_store) = populated_store(COMPANIES)?;
        let (emp_dir, emp_store) = populated_store(EMPLOYEES)?;
        let comp_path = comp_dir.path().to_string_lossy().to_string();
        let emp_path = emp_dir.path().to_string_lossy().to_string();
        drop(comp_store);
        drop(emp_store);

        let query = format!(
            "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            SELECT ?label ?count WHERE {{
                SERVICE <oxigraph://{comp_path}> {{
                    ?company a <http://example.org/Company> ;
                             rdfs:label ?label .
                }}
                SERVICE <oxigraph://{emp_path}> {{
                    ?company <http://example.org/employeeCount> ?count .
                }}
            }}"
        );

        let evaluator =
            SparqlEvaluator::new().with_local_service_handler(LocalStoreServiceHandler::new());
        if let QueryResults::Solutions(solutions) = evaluator
            .parse_query(&query)?
            .on_store(&Store::new()?)
            .execute()?
        {
            let results: Vec<_> = solutions
                .map(|r| {
                    let r = r?;
                    Ok((
                        r.get("label").unwrap().to_string(),
                        r.get("count").unwrap().to_string(),
                    ))
                })
                .collect::<Result<Vec<_>, Box<dyn Error>>>()?;
            assert_eq!(results.len(), 2);
            assert!(results.contains(&(
                "\"Acme Corp\"".to_string(),
                "\"1500\"^^<http://www.w3.org/2001/XMLSchema#integer>".to_string()
            )));
            assert!(results.contains(&(
                "\"Globex Corporation\"".to_string(),
                "\"3200\"^^<http://www.w3.org/2001/XMLSchema#integer>".to_string()
            )));
        } else {
            panic!("Expected solutions");
        }
        Ok(())
    }

    /// Fused path: `SERVICE SILENT` on a missing store falls back to the handler,
    /// which preserves the input bindings (one empty solution).
    #[test]
    fn local_service_fused_silent_on_missing_store() -> Result<(), Box<dyn Error>> {
        let evaluator =
            SparqlEvaluator::new().with_local_service_handler(LocalStoreServiceHandler::new());
        if let QueryResults::Solutions(mut solutions) = evaluator
            .parse_query(
                "SELECT * WHERE { SERVICE SILENT <oxigraph:///nonexistent/path> { ?s ?p ?o } }",
            )?
            .on_store(&Store::new()?)
            .execute()?
        {
            assert!(solutions.next().is_some());
            assert!(solutions.next().is_none());
        }
        Ok(())
    }

    /// Fused path: non-SILENT SERVICE on a missing store still reports the error
    /// inside the solution stream (fallback keeps handler semantics).
    #[test]
    fn local_service_fused_missing_store_is_error() -> Result<(), Box<dyn Error>> {
        let evaluator =
            SparqlEvaluator::new().with_local_service_handler(LocalStoreServiceHandler::new());
        if let QueryResults::Solutions(mut solutions) = evaluator
            .parse_query("SELECT * WHERE { SERVICE <oxigraph:///nonexistent/path> { ?s ?p ?o } }")?
            .on_store(&Store::new()?)
            .execute()?
        {
            let first = solutions.next().unwrap();
            assert!(first.is_err());
        }
        Ok(())
    }

    /// When the outer store already contains a named graph with the same IRI as a
    /// local SERVICE, the fused rewrite must not kick in: the SERVICE keeps its
    /// handler semantics and the outer named graph keeps its own data.
    #[test]
    fn local_service_fused_outer_graph_collision() -> Result<(), Box<dyn Error>> {
        let (dir, store) = populated_store(COMPANIES)?;
        let dir_path = dir.path().to_string_lossy().to_string();
        drop(store);

        // Outer store that contains a named graph with the same IRI as the SERVICE,
        // holding a decoy triple that must NOT be seen as the SERVICE's data.
        let outer = Store::new()?;
        let graph_name = NamedNode::new(format!("oxigraph://{dir_path}"))?;
        outer.insert(Quad::new(
            NamedNode::new("http://example.org/Decoy")?,
            NamedNode::new("http://example.org/name")?,
            Literal::new_simple_literal("DECOY"),
            graph_name.clone(),
        ))?;

        // GRAPH reads the outer named graph; SERVICE reads the local store.
        let query = format!(
            "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            SELECT ?s ?label WHERE {{
                GRAPH <oxigraph://{dir_path}> {{ ?s <http://example.org/name> ?o }}
                SERVICE <oxigraph://{dir_path}> {{ ?company rdfs:label ?label }}
            }}"
        );

        let evaluator =
            SparqlEvaluator::new().with_local_service_handler(LocalStoreServiceHandler::new());
        if let QueryResults::Solutions(solutions) = evaluator
            .parse_query(&query)?
            .on_store(&outer)
            .execute()?
        {
            let results: Vec<_> = solutions
                .map(|r| {
                    let r = r?;
                    Ok((
                        r.get("s").unwrap().to_string(),
                        r.get("label").unwrap().to_string(),
                    ))
                })
                .collect::<Result<Vec<_>, Box<dyn Error>>>()?;
            // Only the decoy triple joins with the two service labels: 2 solutions.
            assert_eq!(results.len(), 2);
            assert!(results
                .iter()
                .all(|(s, _)| s == "<http://example.org/Decoy>"));
        } else {
            panic!("Expected solutions");
        }
        Ok(())
    }

    /// Guard: a SERVICE whose inner pattern contains a GRAPH node is not fused — the
    /// GRAPH addresses named graphs *of the service store* (handler semantics). If the
    /// fused rewrite wrongly kicked in, the GRAPH would resolve against the (empty)
    /// outer dataset and the query would return nothing.
    #[test]
    fn local_service_fused_nested_graph_keeps_handler() -> Result<(), Box<dyn Error>> {
        let (dir, store) = populated_store(COMPANIES)?;
        let dir_path = dir.path().to_string_lossy().to_string();
        // Add a named graph to the service store, distinct from its default graph.
        store.insert(Quad::new(
            NamedNode::new("http://example.org/gs")?,
            NamedNode::new("http://example.org/gp")?,
            Literal::new_simple_literal("in-named-graph"),
            NamedNode::new("http://example.org/g")?,
        ))?;
        drop(store);

        let query = format!(
            "SELECT ?s ?o WHERE {{ SERVICE <oxigraph://{dir_path}> {{ GRAPH <http://example.org/g> {{ ?s ?p ?o }} }} }}"
        );

        let evaluator =
            SparqlEvaluator::new().with_local_service_handler(LocalStoreServiceHandler::new());
        if let QueryResults::Solutions(solutions) = evaluator
            .parse_query(&query)?
            .on_store(&Store::new()?)
            .execute()?
        {
            let results: Vec<_> = solutions
                .map(|r| {
                    let r = r?;
                    Ok((
                        r.get("s").unwrap().to_string(),
                        r.get("o").unwrap().to_string(),
                    ))
                })
                .collect::<Result<Vec<_>, Box<dyn Error>>>()?;
            // The named graph of the *service* store is visible through the handler path.
            assert_eq!(results.len(), 1);
            assert_eq!(results[0], ("<http://example.org/gs>".to_string(), "\"in-named-graph\"".to_string()));
        } else {
            panic!("Expected solutions");
        }
        Ok(())
    }

    /// Guard: a SERVICE with a variable name is not fused. The fused and handler paths
    /// must produce *identical* outcomes for the same variable-name query (here: oxigraph's
    /// evaluator resolves variable service names against the top-level binding, so a
    /// VALUES-bound name surfaces `UnboundService` on the handler path too — the point is
    /// the fused rewrite changes nothing).
    #[test]
    fn local_service_fused_variable_name_keeps_handler() -> Result<(), Box<dyn Error>> {
        let (dir, store) = populated_store(COMPANIES)?;
        let dir_path = dir.path().to_string_lossy().to_string();
        drop(store);

        let query = format!(
            "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            SELECT ?label WHERE {{
                VALUES ?endpoint {{ <oxigraph://{dir_path}> }}
                SERVICE ?endpoint {{ ?company rdfs:label ?label }}
            }}"
        );

        let run = |fused: bool| -> Result<Vec<Result<String, String>>, Box<dyn Error>> {
            let evaluator = if fused {
                SparqlEvaluator::new().with_local_service_handler(LocalStoreServiceHandler::new())
            } else {
                SparqlEvaluator::new()
                    .with_default_service_handler(LocalStoreServiceHandler::new())
            };
            if let QueryResults::Solutions(solutions) = evaluator
                .parse_query(&query)?
                .on_store(&Store::new()?)
                .execute()?
            {
                Ok(solutions
                    .map(|r| {
                        r.map(|r| r.get("label").unwrap().to_string())
                            .map_err(|e| e.to_string())
                    })
                    .collect())
            } else {
                panic!("Expected solutions")
            }
        };

        let handler_results = run(false)?;
        let fused_results = run(true)?;
        assert_eq!(fused_results, handler_results);
        Ok(())
    }

    /// Guard: queries with `FROM NAMED` are not fused — the dataset specification would
    /// gate the fused GRAPH to nothing, while the handler path is independent of it.
    #[test]
    fn local_service_fused_from_named_keeps_handler() -> Result<(), Box<dyn Error>> {
        let (dir, store) = populated_store(COMPANIES)?;
        let dir_path = dir.path().to_string_lossy().to_string();
        drop(store);

        let query = format!(
            "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            SELECT ?label FROM NAMED <oxigraph://{dir_path}> WHERE {{
                SERVICE <oxigraph://{dir_path}> {{ ?company rdfs:label ?label }}
            }}"
        );

        let evaluator =
            SparqlEvaluator::new().with_local_service_handler(LocalStoreServiceHandler::new());
        if let QueryResults::Solutions(solutions) = evaluator
            .parse_query(&query)?
            .on_store(&Store::new()?)
            .execute()?
        {
            let results: Vec<_> = solutions
                .map(|r| {
                    let r = r?;
                    Ok(r.get("label").unwrap().to_string())
                })
                .collect::<Result<Vec<_>, Box<dyn Error>>>()?;
            assert_eq!(results.len(), 2);
        } else {
            panic!("Expected solutions");
        }
        Ok(())
    }

    /// Fused path honours LIMIT: only as many solutions as requested are produced.
    #[test]
    fn local_service_fused_limit() -> Result<(), Box<dyn Error>> {
        let (dir, store) = populated_store(COMPANIES)?;
        let dir_path = dir.path().to_string_lossy().to_string();
        drop(store);

        let query = format!(
            "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            SELECT ?label WHERE {{ SERVICE <oxigraph://{dir_path}> {{ ?company rdfs:label ?label }} }} LIMIT 1"
        );

        let evaluator =
            SparqlEvaluator::new().with_local_service_handler(LocalStoreServiceHandler::new());
        if let QueryResults::Solutions(solutions) = evaluator
            .parse_query(&query)?
            .on_store(&Store::new()?)
            .execute()?
        {
            assert_eq!(solutions.count(), 1);
        } else {
            panic!("Expected solutions");
        }
        Ok(())
    }
}
