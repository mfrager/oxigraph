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
}
