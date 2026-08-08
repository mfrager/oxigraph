use crate::model::{OxString, Term};
#[cfg(feature = "rdf-12")]
use crate::storage::numeric_encoder::EncodedTriple;
use crate::storage::numeric_encoder::{
    Decoder, EncodedTerm, StrHash, StrHashHasher, StrLookup, insert_term,
};
use crate::storage::{CorruptionError, StorageError, StorageReader};
use oxsdatatypes::Boolean;
#[cfg(feature = "rdf-12")]
use spareval::ExpressionTriple;
use spareval::{ExpressionTerm, InternalQuad, QueryableDataset};
use std::cell::RefCell;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::hash::BuildHasherDefault;
#[cfg(feature = "rdf-12")]
use std::sync::Arc;

pub struct DatasetView<'a> {
    reader: StorageReader<'a>,
    extra: RefCell<HashMap<StrHash, OxString, BuildHasherDefault<StrHashHasher>>>,
}

impl<'a> DatasetView<'a> {
    pub fn new(reader: StorageReader<'a>) -> Self {
        Self {
            reader,
            extra: RefCell::new(HashMap::default()),
        }
    }

    pub fn insert_str(&self, key: &StrHash, value: OxString) {
        if let Entry::Vacant(e) = self.extra.borrow_mut().entry(*key) {
            if !matches!(self.reader.contains_str(key), Ok(true)) {
                e.insert(value);
            }
        }
    }
}

impl<'a> QueryableDataset<'a> for DatasetView<'a> {
    type InternalTerm = EncodedTerm;
    type Error = StorageError;

    fn internal_quads_for_pattern(
        &self,
        subject: Option<&EncodedTerm>,
        predicate: Option<&EncodedTerm>,
        object: Option<&EncodedTerm>,
        graph_name: Option<Option<&EncodedTerm>>,
    ) -> impl Iterator<Item = Result<InternalQuad<EncodedTerm>, StorageError>> + use<'a> {
        self.reader
            .quads_for_pattern(
                subject,
                predicate,
                object,
                graph_name.map(|graph_name| graph_name.unwrap_or(&EncodedTerm::DefaultGraph)),
            )
            .map(|quad| {
                let quad = quad?;
                Ok(InternalQuad {
                    subject: quad.subject,
                    predicate: quad.predicate,
                    object: quad.object,
                    graph_name: if quad.graph_name.is_default_graph() {
                        None
                    } else {
                        Some(quad.graph_name)
                    },
                })
            })
    }

    fn internal_named_graphs(
        &self,
    ) -> impl Iterator<Item = Result<EncodedTerm, StorageError>> + use<'a> {
        self.reader.named_graphs()
    }

    fn contains_internal_graph_name(&self, graph_name: &EncodedTerm) -> Result<bool, StorageError> {
        self.reader.contains_named_graph(graph_name)
    }

    fn internalize_term(&self, term: Term) -> Result<EncodedTerm, StorageError> {
        let encoded = (&term).into();
        insert_term(term, &encoded, &mut |key, value| {
            self.insert_str(key, value)
        });
        Ok(encoded)
    }

    fn externalize_term(&self, term: EncodedTerm) -> Result<Term, StorageError> {
        self.decode_term(&term)
    }

    fn externalize_expression_term(
        &self,
        term: EncodedTerm,
    ) -> Result<ExpressionTerm, StorageError> {
        Ok(match term {
            EncodedTerm::DefaultGraph => {
                return Err(CorruptionError::new("Unexpected default graph").into());
            }
            EncodedTerm::BooleanLiteral(value) => ExpressionTerm::BooleanLiteral(value),
            EncodedTerm::FloatLiteral(value) => ExpressionTerm::FloatLiteral(value),
            EncodedTerm::DoubleLiteral(value) => ExpressionTerm::DoubleLiteral(value),
            EncodedTerm::IntegerLiteral(value) => ExpressionTerm::IntegerLiteral(value),
            EncodedTerm::DecimalLiteral(value) => ExpressionTerm::DecimalLiteral(value),
            EncodedTerm::DateTimeLiteral(value) => ExpressionTerm::DateTimeLiteral(value),
            EncodedTerm::TimeLiteral(value) => ExpressionTerm::TimeLiteral(value),
            EncodedTerm::DateLiteral(value) => ExpressionTerm::DateLiteral(value),
            EncodedTerm::GYearMonthLiteral(value) => ExpressionTerm::GYearMonthLiteral(value),
            EncodedTerm::GYearLiteral(value) => ExpressionTerm::GYearLiteral(value),
            EncodedTerm::GMonthDayLiteral(value) => ExpressionTerm::GMonthDayLiteral(value),
            EncodedTerm::GDayLiteral(value) => ExpressionTerm::GDayLiteral(value),
            EncodedTerm::GMonthLiteral(value) => ExpressionTerm::GMonthLiteral(value),
            EncodedTerm::DurationLiteral(value) => ExpressionTerm::DurationLiteral(value),
            EncodedTerm::YearMonthDurationLiteral(value) => {
                ExpressionTerm::YearMonthDurationLiteral(value)
            }
            EncodedTerm::DayTimeDurationLiteral(value) => {
                ExpressionTerm::DayTimeDurationLiteral(value)
            }
            #[cfg(feature = "rdf-12")]
            EncodedTerm::Triple(t) => ExpressionTriple::new(
                self.externalize_expression_term(t.subject.clone())?,
                self.externalize_expression_term(t.predicate.clone())?,
                self.externalize_expression_term(t.object.clone())?,
            )
            .ok_or_else(|| CorruptionError::msg("Invalid triple term in the storage"))?
            .into(),
            _ => self.decode_term(&term)?.into(), // No escape
        })
    }

    fn internalize_expression_term(
        &self,
        term: ExpressionTerm,
    ) -> Result<EncodedTerm, StorageError> {
        Ok(match term {
            ExpressionTerm::BooleanLiteral(value) => EncodedTerm::BooleanLiteral(value),
            ExpressionTerm::FloatLiteral(value) => EncodedTerm::FloatLiteral(value),
            ExpressionTerm::DoubleLiteral(value) => EncodedTerm::DoubleLiteral(value),
            ExpressionTerm::IntegerLiteral(value) => EncodedTerm::IntegerLiteral(value),
            ExpressionTerm::DecimalLiteral(value) => EncodedTerm::DecimalLiteral(value),
            ExpressionTerm::DateTimeLiteral(value) => EncodedTerm::DateTimeLiteral(value),
            ExpressionTerm::TimeLiteral(value) => EncodedTerm::TimeLiteral(value),
            ExpressionTerm::DateLiteral(value) => EncodedTerm::DateLiteral(value),
            ExpressionTerm::GYearMonthLiteral(value) => EncodedTerm::GYearMonthLiteral(value),
            ExpressionTerm::GYearLiteral(value) => EncodedTerm::GYearLiteral(value),
            ExpressionTerm::GMonthDayLiteral(value) => EncodedTerm::GMonthDayLiteral(value),
            ExpressionTerm::GDayLiteral(value) => EncodedTerm::GDayLiteral(value),
            ExpressionTerm::GMonthLiteral(value) => EncodedTerm::GMonthLiteral(value),
            ExpressionTerm::DurationLiteral(value) => EncodedTerm::DurationLiteral(value),
            ExpressionTerm::YearMonthDurationLiteral(value) => {
                EncodedTerm::YearMonthDurationLiteral(value)
            }
            ExpressionTerm::DayTimeDurationLiteral(value) => {
                EncodedTerm::DayTimeDurationLiteral(value)
            }
            #[cfg(feature = "rdf-12")]
            ExpressionTerm::Triple(t) => EncodedTerm::Triple(Arc::new(EncodedTriple {
                subject: self.internalize_expression_term(t.subject.into())?,
                predicate: self.internalize_expression_term(t.predicate.into())?,
                object: self.internalize_expression_term(t.object)?,
            })),
            _ => self.internalize_term(term.into())?, // No fast path
        })
    }

    fn internal_term_effective_boolean_value(
        &self,
        term: EncodedTerm,
    ) -> Result<Option<bool>, StorageError> {
        Ok(match term {
            EncodedTerm::BooleanLiteral(value) => Some(value.into()),
            EncodedTerm::SmallStringLiteral(value) => Some(!value.is_empty()),
            EncodedTerm::BigStringLiteral { .. } => {
                Some(false) // A big literal can't be empty
            }
            EncodedTerm::FloatLiteral(value) => Some(Boolean::from(value).into()),
            EncodedTerm::DoubleLiteral(value) => Some(Boolean::from(value).into()),
            EncodedTerm::IntegerLiteral(value) => Some(Boolean::from(value).into()),
            EncodedTerm::DecimalLiteral(value) => Some(Boolean::from(value).into()),
            _ => None,
        })
    }
}

impl StrLookup for DatasetView<'_> {
    fn get_str(&self, key: &StrHash) -> Result<Option<OxString>, StorageError> {
        Ok(if let Some(value) = self.extra.borrow().get(key) {
            Some(value.clone())
        } else {
            self.reader.get_str(key)?
        })
    }
}

/// A snapshot of a local `SERVICE <oxigraph://...>` store, exposed as a virtual named graph
/// of a [`FusedDataset`].
///
/// Service snapshots are `'static` (RocksDB snapshots are owned), and because
/// [`EncodedTerm`]s are keyless hashes of the term strings, the terms stored in a service
/// store are identical to the ones of any other oxigraph store: they flow through the
/// fused dataset by identity, with no externalization/internalization round-trip
/// (see planning/service_boundary_design.md, R1).
pub(crate) struct FusedService {
    /// The encoded virtual graph name (the `oxigraph://` SERVICE IRI).
    graph_name: EncodedTerm,
    /// The snapshot of the service store. Its default graph is the SERVICE's dataset.
    view: DatasetView<'static>,
}

impl FusedService {
    pub(crate) fn new(graph_name: EncodedTerm, view: DatasetView<'static>) -> Self {
        Self { graph_name, view }
    }

    pub(crate) fn graph_name(&self) -> &EncodedTerm {
        &self.graph_name
    }
}

/// Internal implementation detail of [`crate::sparql::PreparedSparqlQuery::on_store`] and
/// [`crate::sparql::PreparedSparqlQuery::on_transaction`]: the dataset used to evaluate a query
/// whose local `SERVICE <oxigraph://...>` patterns have been fused into virtual graphs.
///
/// `SERVICE <oxigraph://path> { pattern }` patterns are rewritten into
/// `GRAPH <oxigraph://path> { pattern }` by [`crate::sparql::local_service::fuse_query`];
/// evaluating such a `GRAPH` pattern against this dataset routes the quad scans to the
/// service store's snapshot, and terms pass through by identity (same `EncodedTerm` hash
/// in every store). Everything else is delegated to the outer [`DatasetView`].
///
/// This type is `pub` because it appears in the public return type of `on_store`/`on_transaction`
/// (like [`DatasetView`]), but it is not meant to be used directly: its fields are private and
/// all its methods are `pub(crate)`.
pub struct FusedDataset<'a> {
    outer: DatasetView<'a>,
    services: Vec<FusedService>,
    extra: RefCell<HashMap<StrHash, OxString, BuildHasherDefault<StrHashHasher>>>,
}

impl<'a> FusedDataset<'a> {
    pub(crate) fn new(outer: DatasetView<'a>, services: Vec<FusedService>) -> Self {
        Self {
            outer,
            services,
            extra: RefCell::new(HashMap::default()),
        }
    }

    fn find_service(&self, graph_name: &EncodedTerm) -> Option<&DatasetView<'static>> {
        self.services
            .iter()
            .find(|service| &service.graph_name == graph_name)
            .map(|service| &service.view)
    }

    /// Like [`DatasetView::insert_str`] but also checks the fused service stores: a string
    /// already available in any of them does not need to be duplicated in the overlay.
    fn insert_str(&self, key: &StrHash, value: OxString) {
        if let Entry::Vacant(e) = self.extra.borrow_mut().entry(*key) {
            if !matches!(self.outer.reader.contains_str(key), Ok(true))
                && !self
                    .services
                    .iter()
                    .any(|service| matches!(service.view.reader.contains_str(key), Ok(true)))
            {
                e.insert(value);
            }
        }
    }
}

impl<'a> QueryableDataset<'a> for FusedDataset<'a> {
    type InternalTerm = EncodedTerm;
    type Error = StorageError;

    fn internal_quads_for_pattern(
        &self,
        subject: Option<&EncodedTerm>,
        predicate: Option<&EncodedTerm>,
        object: Option<&EncodedTerm>,
        graph_name: Option<Option<&EncodedTerm>>,
    ) -> impl Iterator<Item = Result<InternalQuad<EncodedTerm>, StorageError>> + use<'a> {
        // A virtual graph routes to the service store (its default graph); everything else
        // is delegated to the outer dataset (default graph, outer named graphs, and the
        // union-of-named-graphs case, which intentionally does not include service data).
        let iter: Box<dyn Iterator<Item = Result<InternalQuad<EncodedTerm>, StorageError>> + 'a> =
            if let Some(Some(graph_name)) = graph_name {
                if let Some(service) = self.find_service(graph_name) {
                    // The SERVICE's default graph is exposed as the virtual named graph:
                    // stamp the graph name on every quad so that graph-aware evaluation
                    // (e.g. quad patterns with an explicit graph name, pushed down by the
                    // optimizer) sees the graph it asked for.
                    let graph_name = graph_name.clone();
                    Box::new(
                        service
                            .internal_quads_for_pattern(subject, predicate, object, Some(None))
                            .map(move |quad| {
                                let mut quad = quad?;
                                quad.graph_name = Some(graph_name.clone());
                                Ok(quad)
                            }),
                    )
                } else {
                    Box::new(self.outer.internal_quads_for_pattern(
                        subject,
                        predicate,
                        object,
                        Some(Some(graph_name)),
                    ))
                }
            } else {
                Box::new(self.outer.internal_quads_for_pattern(
                    subject,
                    predicate,
                    object,
                    graph_name,
                ))
            };
        iter
    }

    fn internal_named_graphs(
        &self,
    ) -> impl Iterator<Item = Result<EncodedTerm, StorageError>> + use<'a> {
        // Virtual service graphs are intentionally NOT enumerated here: like the handler
        // path, `GRAPH ?g` only iterates the outer dataset's named graphs. Constant fused
        // graph names are still routed through `internal_quads_for_pattern` and
        // `contains_internal_graph_name`.
        self.outer.internal_named_graphs()
    }

    fn contains_internal_graph_name(&self, graph_name: &EncodedTerm) -> Result<bool, StorageError> {
        if self.find_service(graph_name).is_some() {
            Ok(true)
        } else {
            self.outer.contains_internal_graph_name(graph_name)
        }
    }

    fn internalize_term(&self, term: Term) -> Result<EncodedTerm, StorageError> {
        let encoded = (&term).into();
        insert_term(term, &encoded, &mut |key, value| self.insert_str(key, value));
        Ok(encoded)
    }

    fn externalize_term(&self, term: EncodedTerm) -> Result<Term, StorageError> {
        self.decode_term(&term)
    }

    fn externalize_expression_term(
        &self,
        term: EncodedTerm,
    ) -> Result<ExpressionTerm, StorageError> {
        Ok(match term {
            EncodedTerm::DefaultGraph => {
                return Err(CorruptionError::new("Unexpected default graph").into());
            }
            EncodedTerm::BooleanLiteral(value) => ExpressionTerm::BooleanLiteral(value),
            EncodedTerm::FloatLiteral(value) => ExpressionTerm::FloatLiteral(value),
            EncodedTerm::DoubleLiteral(value) => ExpressionTerm::DoubleLiteral(value),
            EncodedTerm::IntegerLiteral(value) => ExpressionTerm::IntegerLiteral(value),
            EncodedTerm::DecimalLiteral(value) => ExpressionTerm::DecimalLiteral(value),
            EncodedTerm::DateTimeLiteral(value) => ExpressionTerm::DateTimeLiteral(value),
            EncodedTerm::TimeLiteral(value) => ExpressionTerm::TimeLiteral(value),
            EncodedTerm::DateLiteral(value) => ExpressionTerm::DateLiteral(value),
            EncodedTerm::GYearMonthLiteral(value) => ExpressionTerm::GYearMonthLiteral(value),
            EncodedTerm::GYearLiteral(value) => ExpressionTerm::GYearLiteral(value),
            EncodedTerm::GMonthDayLiteral(value) => ExpressionTerm::GMonthDayLiteral(value),
            EncodedTerm::GDayLiteral(value) => ExpressionTerm::GDayLiteral(value),
            EncodedTerm::GMonthLiteral(value) => ExpressionTerm::GMonthLiteral(value),
            EncodedTerm::DurationLiteral(value) => ExpressionTerm::DurationLiteral(value),
            EncodedTerm::YearMonthDurationLiteral(value) => {
                ExpressionTerm::YearMonthDurationLiteral(value)
            }
            EncodedTerm::DayTimeDurationLiteral(value) => {
                ExpressionTerm::DayTimeDurationLiteral(value)
            }
            #[cfg(feature = "rdf-12")]
            EncodedTerm::Triple(t) => ExpressionTriple::new(
                self.externalize_expression_term(t.subject.clone())?,
                self.externalize_expression_term(t.predicate.clone())?,
                self.externalize_expression_term(t.object.clone())?,
            )
            .ok_or_else(|| CorruptionError::msg("Invalid triple term in the storage"))?
            .into(),
            _ => self.decode_term(&term)?.into(), // No escape
        })
    }

    fn internalize_expression_term(
        &self,
        term: ExpressionTerm,
    ) -> Result<EncodedTerm, StorageError> {
        Ok(match term {
            ExpressionTerm::BooleanLiteral(value) => EncodedTerm::BooleanLiteral(value),
            ExpressionTerm::FloatLiteral(value) => EncodedTerm::FloatLiteral(value),
            ExpressionTerm::DoubleLiteral(value) => EncodedTerm::DoubleLiteral(value),
            ExpressionTerm::IntegerLiteral(value) => EncodedTerm::IntegerLiteral(value),
            ExpressionTerm::DecimalLiteral(value) => EncodedTerm::DecimalLiteral(value),
            ExpressionTerm::DateTimeLiteral(value) => EncodedTerm::DateTimeLiteral(value),
            ExpressionTerm::TimeLiteral(value) => EncodedTerm::TimeLiteral(value),
            ExpressionTerm::DateLiteral(value) => EncodedTerm::DateLiteral(value),
            ExpressionTerm::GYearMonthLiteral(value) => EncodedTerm::GYearMonthLiteral(value),
            ExpressionTerm::GYearLiteral(value) => EncodedTerm::GYearLiteral(value),
            ExpressionTerm::GMonthDayLiteral(value) => EncodedTerm::GMonthDayLiteral(value),
            ExpressionTerm::GDayLiteral(value) => EncodedTerm::GDayLiteral(value),
            ExpressionTerm::GMonthLiteral(value) => EncodedTerm::GMonthLiteral(value),
            ExpressionTerm::DurationLiteral(value) => EncodedTerm::DurationLiteral(value),
            ExpressionTerm::YearMonthDurationLiteral(value) => {
                EncodedTerm::YearMonthDurationLiteral(value)
            }
            ExpressionTerm::DayTimeDurationLiteral(value) => {
                EncodedTerm::DayTimeDurationLiteral(value)
            }
            #[cfg(feature = "rdf-12")]
            ExpressionTerm::Triple(t) => EncodedTerm::Triple(Arc::new(EncodedTriple {
                subject: self.internalize_expression_term(t.subject.into())?,
                predicate: self.internalize_expression_term(t.predicate.into())?,
                object: self.internalize_expression_term(t.object)?,
            })),
            _ => self.internalize_term(term.into())?, // No fast path
        })
    }

    fn internal_term_effective_boolean_value(
        &self,
        term: EncodedTerm,
    ) -> Result<Option<bool>, StorageError> {
        Ok(match term {
            EncodedTerm::BooleanLiteral(value) => Some(value.into()),
            EncodedTerm::SmallStringLiteral(value) => Some(!value.is_empty()),
            EncodedTerm::BigStringLiteral { .. } => {
                Some(false) // A big literal can't be empty
            }
            EncodedTerm::FloatLiteral(value) => Some(Boolean::from(value).into()),
            EncodedTerm::DoubleLiteral(value) => Some(Boolean::from(value).into()),
            EncodedTerm::IntegerLiteral(value) => Some(Boolean::from(value).into()),
            EncodedTerm::DecimalLiteral(value) => Some(Boolean::from(value).into()),
            _ => None,
        })
    }
}

impl StrLookup for FusedDataset<'_> {
    fn get_str(&self, key: &StrHash) -> Result<Option<OxString>, StorageError> {
        if let Some(value) = self.extra.borrow().get(key) {
            Ok(Some(value.clone()))
        } else if let Some(value) = self.outer.get_str(key)? {
            Ok(Some(value))
        } else {
            for service in &self.services {
                if let Some(value) = service.view.get_str(key)? {
                    return Ok(Some(value));
                }
            }
            Ok(None)
        }
    }
}
