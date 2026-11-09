//! Logical keyspaces of the SlateDB backend.
//!
//! Oxigraph's RocksDB backend keeps its eleven RDF indexes in separate column families and its
//! format version in the `default` one. SlateDB has a single key space, so each logical index gets
//! a one-byte namespace prefix:
//!
//! ```text
//! stored key = [namespace] || oxigraph key
//! ```
//!
//! Prepending a constant byte preserves the byte order of the key that follows it, which is all
//! the RDF indexes rely on, and it turns every index scan into a scan bounded to one namespace.
//!
//! The namespace bytes are part of the on-disk format: never renumber them and never reuse one.
#![cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "the remaining helpers are consumed by the RDF layer added in phase 2"
    )
)]

/// A logical keyspace, mirroring the RocksDB column families so that a migration is a prefix map.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub(crate) enum Ns {
    /// Format metadata (`oxversion` and friends), the RocksDB `default` column family.
    Meta = 0x00,
    Id2Str = 0x01,
    Spog = 0x02,
    Posg = 0x03,
    Ospg = 0x04,
    Gspo = 0x05,
    Gpos = 0x06,
    Gosp = 0x07,
    Dspo = 0x08,
    Dpos = 0x09,
    Dosp = 0x0A,
    Graphs = 0x0B,
}

impl Ns {
    /// The prefix byte of this keyspace.
    #[inline]
    pub(crate) const fn as_byte(self) -> u8 {
        self as u8
    }

    /// The keyspace of a prefix byte, or `None` if no keyspace uses it.
    ///
    /// Only the RocksDB migration reads a keyspace back from a byte.
    #[inline]
    #[cfg(feature = "rocksdb")]
    pub(crate) const fn from_byte(byte: u8) -> Option<Self> {
        Some(match byte {
            0x00 => Self::Meta,
            0x01 => Self::Id2Str,
            0x02 => Self::Spog,
            0x03 => Self::Posg,
            0x04 => Self::Ospg,
            0x05 => Self::Gspo,
            0x06 => Self::Gpos,
            0x07 => Self::Gosp,
            0x08 => Self::Dspo,
            0x09 => Self::Dpos,
            0x0A => Self::Dosp,
            0x0B => Self::Graphs,
            _ => return None,
        })
    }
}

/// Builds the stored key of `key` in `ns`.
pub(crate) fn prefixed(ns: Ns, key: &[u8]) -> Vec<u8> {
    prefixed_byte(ns.as_byte(), key)
}

/// Same as [`prefixed`], reusing `buffer` to avoid an allocation per written key.
pub(crate) fn prefixed_into(buffer: &mut Vec<u8>, ns: Ns, key: &[u8]) {
    buffer.clear();
    buffer.reserve(1 + key.len());
    buffer.push(ns.as_byte());
    buffer.extend_from_slice(key);
}

/// Returns the Oxigraph key part of a stored key, or `None` if it belongs to another keyspace.
pub(crate) fn strip_prefix(ns: Ns, stored_key: &[u8]) -> Option<&[u8]> {
    match stored_key.split_first() {
        Some((namespace, key)) if *namespace == ns.as_byte() => Some(key),
        _ => None,
    }
}

/// Bounds of every stored key of `ns` whose Oxigraph key starts with `prefix`.
///
/// The range is half-open: `start..end`, where `end` is `None` when the range runs to the end of the
/// keyspace. Both bounds are complete stored keys, so the range can be handed as is to a range scan.
pub(crate) fn prefix_bounds(ns: Ns, prefix: &[u8]) -> (Vec<u8>, Option<Vec<u8>>) {
    (prefixed(ns, prefix), upper_bound(ns.as_byte(), prefix))
}

fn prefixed_byte(ns: u8, key: &[u8]) -> Vec<u8> {
    let mut buffer = Vec::with_capacity(1 + key.len());
    buffer.push(ns);
    buffer.extend_from_slice(key);
    buffer
}

/// Exclusive upper bound of the keys of keyspace `ns` starting with `prefix`.
fn upper_bound(ns: u8, prefix: &[u8]) -> Option<Vec<u8>> {
    match prefix.iter().rposition(|byte| *byte < u8::MAX) {
        Some(index) => {
            let mut end = prefixed_byte(ns, &prefix[..=index]);
            // `index + 1` skips the namespace byte and is the last byte copied, known not to be
            // `u8::MAX`, so the increment cannot overflow.
            end[index + 1] += 1;
            Some(end)
        }
        // The prefix is empty or made only of `u8::MAX` bytes: the matching keys are the tail of
        // the keyspace, so the range ends where the keyspace ends.
        None => ns
            .checked_add(1)
            .map(|next_namespace| prefixed_byte(next_namespace, &[])),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every keyspace, in namespace byte order.
    const ALL: [Ns; 12] = [
        Ns::Meta,
        Ns::Id2Str,
        Ns::Spog,
        Ns::Posg,
        Ns::Ospg,
        Ns::Gspo,
        Ns::Gpos,
        Ns::Gosp,
        Ns::Dspo,
        Ns::Dpos,
        Ns::Dosp,
        Ns::Graphs,
    ];

    #[test]
    fn namespace_bytes_are_stable() {
        // These values are written on disk. Changing one is a data format break.
        assert_eq!(ALL.map(Ns::as_byte), [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]);
    }

    #[test]
    fn prepending_preserves_the_order_of_the_oxigraph_keys() {
        let keys: [&[u8]; 7] = [
            b"",
            b"\x00",
            b"\x00\x01",
            b"\x01",
            b"abc",
            b"\xff",
            b"\xff\x00",
        ];
        let mut sorted = keys.to_vec();
        sorted.sort_unstable();

        let mut prefixed_keys = keys.map(|key| prefixed(Ns::Spog, key)).to_vec();
        prefixed_keys.sort_unstable();
        assert_eq!(
            prefixed_keys,
            sorted
                .into_iter()
                .map(|key| prefixed(Ns::Spog, key))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn stored_keys_are_the_namespace_followed_by_the_oxigraph_key() {
        let keys: [&[u8]; 4] = [b"", b"\x00", b"\xff\xff\xff", b"http://example.com"];
        for key in keys {
            let stored = prefixed(Ns::Posg, key);
            assert_eq!(stored.len(), 1 + key.len());
            assert_eq!(stored[0], Ns::Posg.as_byte());
            assert_eq!(&stored[1..], key);
        }
    }

    #[test]
    fn keyspaces_do_not_interleave() {
        // The largest possible key of a keyspace sorts before the smallest key of the next one, so
        // a scan bounded to a keyspace can never reach another one.
        for keyspace in ALL {
            let next_keyspace = vec![keyspace.as_byte() + 1];
            assert!(
                prefixed(keyspace, &[u8::MAX; 32]) < next_keyspace,
                "{keyspace:?}"
            );
            assert_eq!(
                prefix_bounds(keyspace, &[u8::MAX; 32]).1,
                Some(next_keyspace),
                "{keyspace:?}"
            );
        }
    }

    #[test]
    fn prefix_bounds_of_an_empty_prefix_cover_the_whole_keyspace() {
        for keyspace in ALL {
            let (start, end) = prefix_bounds(keyspace, &[]);
            assert_eq!(start, vec![keyspace.as_byte()]);
            assert_eq!(end, Some(vec![keyspace.as_byte() + 1]), "{keyspace:?}");
        }
    }

    #[test]
    fn prefix_bounds_increment_the_last_incrementable_byte() {
        let (start, end) = prefix_bounds(Ns::Dspo, b"ab");
        assert_eq!(start, b"\x08ab".to_vec());
        assert_eq!(end, Some(b"\x08ac".to_vec()));

        // Only the last byte that is not `u8::MAX` is incremented, and the rest is dropped.
        let (start, end) = prefix_bounds(Ns::Dspo, &[0x01, u8::MAX, u8::MAX]);
        assert_eq!(start, vec![0x08, 0x01, u8::MAX, u8::MAX]);
        assert_eq!(end, Some(vec![0x08, 0x02]));
    }

    #[test]
    fn prefix_bounds_of_a_saturated_prefix_stop_at_the_keyspace_end() {
        let (start, end) = prefix_bounds(Ns::Dspo, &[u8::MAX, u8::MAX]);
        assert_eq!(start, vec![0x08, u8::MAX, u8::MAX]);
        assert_eq!(end, Some(vec![0x09]));
    }

    #[test]
    fn upper_bound_has_no_value_after_the_last_keyspace() {
        assert_eq!(upper_bound(u8::MAX, &[]), None);
        assert_eq!(upper_bound(u8::MAX, &[u8::MAX]), None);
        assert_eq!(upper_bound(u8::MAX - 1, &[]), Some(vec![u8::MAX]));
    }

    #[test]
    fn strip_prefix_returns_the_oxigraph_key() {
        assert_eq!(
            strip_prefix(Ns::Id2Str, b"\x01abc"),
            Some(b"abc".as_slice())
        );
        assert_eq!(strip_prefix(Ns::Id2Str, b"\x01"), Some(b"".as_slice()));
        // Another keyspace, and a key too short to hold a namespace.
        assert_eq!(strip_prefix(Ns::Id2Str, b"\x02abc"), None);
        assert_eq!(strip_prefix(Ns::Id2Str, b""), None);
    }

    #[test]
    fn prefixed_into_reuses_the_buffer() {
        let mut buffer = Vec::with_capacity(64);
        let capacity = buffer.capacity();

        prefixed_into(&mut buffer, Ns::Posg, b"a-longer-oxigraph-key");
        assert_eq!(buffer, prefixed(Ns::Posg, b"a-longer-oxigraph-key"));

        // A shorter key must leave no trailing bytes of the previous one.
        prefixed_into(&mut buffer, Ns::Posg, b"short");
        assert_eq!(buffer, prefixed(Ns::Posg, b"short"));
        assert_eq!(buffer.capacity(), capacity);
    }
}
