//! Encoding of a batch of quads for the bulk loaders.
//!
//! Both on-disk backends need the same thing out of a batch of quads: the sorted, deduplicated key
//! list of every index, plus the `id2str` entries of the terms that are not encoded inline. The
//! RocksDB loader writes them as SST files and ingests those; the SlateDB loader puts them into a
//! write batch. Extracting the sort/dedup core here keeps the two loaders from drifting apart
//! (planning/slatedb/implementation_plan2.md, D8).

use crate::model::{GraphName, Quad, Term};
use crate::storage::binary_encoder::{encode_term, encode_term_quad, encode_term_triple};
use crate::storage::error::StorageError;
use crate::storage::numeric_encoder::{EncodedQuad, EncodedTerm, insert_term};
use spareval::CancellationToken;

/// Rough per-key overhead of the containers the loaders stage the keys into, used to keep the
/// atomic bulk load budget honest: a `Vec<u8>` key costs more than its length.
#[cfg(feature = "slatedb")]
const STAGED_KEY_OVERHEAD: u64 = 24;

/// The keys of one batch of quads, sorted and deduplicated per index.
#[derive(Default)]
pub(crate) struct BatchedKeys {
    /// `id2str` entries as `(key, string)`, sorted and deduplicated by key.
    pub id2str: Vec<([u8; 16], Box<str>)>,
    pub dspo: Vec<Vec<u8>>,
    pub dpos: Vec<Vec<u8>>,
    pub dosp: Vec<Vec<u8>>,
    pub spog: Vec<Vec<u8>>,
    pub posg: Vec<Vec<u8>>,
    pub ospg: Vec<Vec<u8>>,
    pub gspo: Vec<Vec<u8>>,
    pub gpos: Vec<Vec<u8>>,
    pub gosp: Vec<Vec<u8>>,
    pub graphs: Vec<Vec<u8>>,
    /// Number of distinct quads of the batch, which is what the progress hooks count.
    pub quads: u64,
}

impl BatchedKeys {
    fn new(batch_size: usize) -> Self {
        Self {
            id2str: Vec::with_capacity(3 * batch_size),
            dspo: Vec::with_capacity(batch_size),
            dpos: Vec::with_capacity(batch_size),
            dosp: Vec::with_capacity(batch_size),
            spog: Vec::with_capacity(batch_size),
            posg: Vec::with_capacity(batch_size),
            ospg: Vec::with_capacity(batch_size),
            gspo: Vec::with_capacity(batch_size),
            gpos: Vec::with_capacity(batch_size),
            gosp: Vec::with_capacity(batch_size),
            graphs: Vec::new(),
            quads: 0,
        }
    }

    /// Encodes `quads`, sorting and deduplicating every index.
    pub(crate) fn encode(
        quads: Vec<Quad>,
        cancellation_token: &CancellationToken,
    ) -> Result<Self, StorageError> {
        let mut keys = Self::new(quads.len());
        for quad in quads {
            let encoded = EncodedQuad::from(&quad);
            if quad.graph_name.is_default_graph() {
                keys.dspo.push(encode_term_triple(
                    &encoded.subject,
                    &encoded.predicate,
                    &encoded.object,
                ));
                keys.dpos.push(encode_term_triple(
                    &encoded.predicate,
                    &encoded.object,
                    &encoded.subject,
                ));
                keys.dosp.push(encode_term_triple(
                    &encoded.object,
                    &encoded.subject,
                    &encoded.predicate,
                ));
                keys.insert_term(quad.subject.into(), &encoded.subject);
                keys.insert_term(quad.predicate.into(), &encoded.predicate);
                keys.insert_term(quad.object, &encoded.object);
            } else {
                keys.spog.push(encode_term_quad(
                    &encoded.subject,
                    &encoded.predicate,
                    &encoded.object,
                    &encoded.graph_name,
                ));
                keys.posg.push(encode_term_quad(
                    &encoded.predicate,
                    &encoded.object,
                    &encoded.subject,
                    &encoded.graph_name,
                ));
                keys.ospg.push(encode_term_quad(
                    &encoded.object,
                    &encoded.subject,
                    &encoded.predicate,
                    &encoded.graph_name,
                ));
                keys.gspo.push(encode_term_quad(
                    &encoded.graph_name,
                    &encoded.subject,
                    &encoded.predicate,
                    &encoded.object,
                ));
                keys.gpos.push(encode_term_quad(
                    &encoded.graph_name,
                    &encoded.predicate,
                    &encoded.object,
                    &encoded.subject,
                ));
                keys.gosp.push(encode_term_quad(
                    &encoded.graph_name,
                    &encoded.object,
                    &encoded.subject,
                    &encoded.predicate,
                ));
                keys.graphs.push(encode_term(&encoded.graph_name));
                keys.insert_term(quad.subject.into(), &encoded.subject);
                keys.insert_term(quad.predicate.into(), &encoded.predicate);
                keys.insert_term(quad.object, &encoded.object);
                keys.insert_graph_name(quad.graph_name, &encoded.graph_name);
            }
        }
        if cancellation_token.is_cancelled() {
            return Err(StorageError::Other("Cancelled".into()));
        }
        keys.finish();
        Ok(keys)
    }

    /// Memory the batch will occupy once staged, including the container overhead.
    #[cfg(feature = "slatedb")]
    pub(crate) fn staged_bytes(&self) -> u64 {
        let mut bytes = 0;
        for keys in [
            &self.dspo,
            &self.dpos,
            &self.dosp,
            &self.spog,
            &self.posg,
            &self.ospg,
            &self.gspo,
            &self.gpos,
            &self.gosp,
            &self.graphs,
        ] {
            for key in keys {
                bytes += key.len() as u64 + STAGED_KEY_OVERHEAD;
            }
        }
        for (key, value) in &self.id2str {
            bytes += key.len() as u64 + value.len() as u64 + STAGED_KEY_OVERHEAD;
        }
        bytes
    }

    fn finish(&mut self) {
        self.id2str.sort_unstable();
        self.id2str.dedup_by(|(a, _), (b, _)| a == b);
        for keys in [
            &mut self.dspo,
            &mut self.dpos,
            &mut self.dosp,
            &mut self.spog,
            &mut self.posg,
            &mut self.ospg,
            &mut self.gspo,
            &mut self.gpos,
            &mut self.gosp,
            &mut self.graphs,
        ] {
            keys.sort_unstable();
            keys.dedup();
        }
        self.quads = (self.dspo.len() + self.spog.len()) as u64;
    }

    fn insert_term(&mut self, term: Term, encoded: &EncodedTerm) {
        let id2str = &mut self.id2str;
        insert_term(term, encoded, &mut |key, value| {
            id2str.push((key.to_be_bytes(), value.as_str().into()));
        });
    }

    fn insert_graph_name(&mut self, graph_name: GraphName, encoded: &EncodedTerm) {
        match graph_name {
            GraphName::NamedNode(graph_name) => self.insert_term(graph_name.into(), encoded),
            GraphName::BlankNode(graph_name) => self.insert_term(graph_name.into(), encoded),
            GraphName::DefaultGraph => (),
        }
    }
}
