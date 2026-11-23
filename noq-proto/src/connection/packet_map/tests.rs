use alloc::{collections::BTreeMap, vec::Vec};

use proptest::{collection::vec, prelude::*};
use test_strategy::proptest;

use super::PacketMap;

/// Checks the structure and the memory bound of `map`.
fn check_invariants<T>(map: &PacketMap<T>) {
    let mut previous_last = None;
    let last_key = map.runs.last_key_value().map(|(&key, _)| key);
    for (&key, span) in &map.runs {
        assert!(key <= span.start, "key {key} above start {}", span.start);
        if let Some(previous_last) = previous_last {
            assert!(previous_last < key, "runs overlap at key {key}");
        }
        previous_last = Some(span.last());

        let (len, capacity) = (span.values.len(), span.values.capacity());
        if Some(key) == last_key {
            assert!(
                capacity <= 16 || capacity < 4 * len,
                "last run holds {len} values in {capacity} slots"
            );
        } else {
            assert!(
                capacity < 2 * len || (len == 1 && capacity == 1),
                "run holds {len} values in {capacity} slots"
            );
        }
    }
}

/// The number of value slots allocated or stored inline.
fn capacity<T>(map: &PacketMap<T>) -> usize {
    map.runs.values().map(|span| span.values.capacity()).sum()
}

fn entries(map: &PacketMap<u64>) -> Vec<(u64, u64)> {
    map.iter().map(|(pn, &value)| (pn, value)).collect()
}

#[derive(Debug, Clone)]
enum Op {
    /// Inserts after skipping `gap` packet numbers.
    Append {
        gap: u64,
    },
    /// Removes the n-th entry, modulo the number of entries.
    RemoveEntry(usize),
    /// Removes a packet number that may or may not be present.
    Remove(u64),
    Get(u64),
    /// Iterates over `start..end`, which may be empty or reversed.
    Range(u64, u64),
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        8 => prop_oneof![8 => Just(0u64), 1 => 1..4u64, 1 => 64..200u64]
            .prop_map(|gap| Op::Append { gap }),
        5 => any::<usize>().prop_map(Op::RemoveEntry),
        1 => (0..3000u64).prop_map(Op::Remove),
        1 => (0..3000u64).prop_map(Op::Get),
        1 => (0..3000u64, 0..3000u64).prop_map(|(start, end)| Op::Range(start, end)),
    ]
}

#[proptest]
fn matches_btree_map(#[strategy(vec(op(), 0..400))] ops: Vec<Op>) {
    let mut map = PacketMap::new();
    let mut reference = BTreeMap::new();
    let mut next = 0u64;
    for op in ops {
        match op {
            Op::Append { gap } => {
                let pn = next + gap;
                let value = pn.wrapping_mul(0x9e37_79b9);
                map.insert(pn, value).unwrap();
                reference.insert(pn, value);
                next = pn + 1;
            }
            Op::RemoveEntry(n) => {
                if !reference.is_empty() {
                    let pn = *reference.keys().nth(n % reference.len()).unwrap();
                    assert_eq!(map.remove(pn), reference.remove(&pn));
                }
            }
            Op::Remove(pn) => assert_eq!(map.remove(pn), reference.remove(&pn)),
            Op::Get(pn) => assert_eq!(map.get(pn), reference.get(&pn)),
            Op::Range(start, end) => {
                let actual = map.iter_range(start..end).map(|(pn, &value)| (pn, value));
                // `BTreeMap::range` panics on a reversed range, which is empty.
                let expected = reference
                    .range(start..end.max(start))
                    .map(|(&pn, &value)| (pn, value));
                assert_eq!(actual.collect::<Vec<_>>(), expected.collect::<Vec<_>>());
            }
        }
        check_invariants(&map);
        assert_eq!(
            entries(&map),
            reference
                .iter()
                .map(|(&pn, &v)| (pn, v))
                .collect::<Vec<_>>()
        );
    }
    let values: Vec<_> = map.values_mut().map(|value| *value).collect();
    assert_eq!(values, reference.values().copied().collect::<Vec<_>>());
    assert_eq!(
        map.into_iter().collect::<Vec<_>>(),
        reference.into_iter().collect::<Vec<_>>()
    );
}

/// One old packet stays while a tail of later packets slides forward behind it.
#[test]
fn pinned_packet_with_sliding_tail() {
    const TAIL: usize = 1000;
    let mut map = PacketMap::new();
    map.insert(0, 0u64).unwrap();
    for pn in 1..100_000 {
        map.insert(pn, pn).unwrap();
        if pn as usize > TAIL {
            let (oldest, _) = map.iter_range(1..u64::MAX).next().unwrap();
            map.remove(oldest);
        }
        check_invariants(&map);
    }
    assert_eq!(map.iter().count(), TAIL + 1);
    assert_eq!(map.runs.len(), 2);
    assert!(capacity(&map) <= 4 * (TAIL + 1));
}

/// An ACK frame pokes evenly spaced holes into a long run.
#[test]
fn holes_in_long_run() {
    let mut map = PacketMap::new();
    for pn in 0..10_000u64 {
        map.insert(pn, pn).unwrap();
    }
    for pn in (50..10_000).step_by(100) {
        assert_eq!(map.remove(pn), Some(pn));
        check_invariants(&map);
    }
    assert_eq!(map.runs.len(), 101);
    assert_eq!(map.iter().count(), 9_900);
    assert!(capacity(&map) <= 2 * 9_900);
}

/// Every other packet is removed, leaving only isolated packets.
#[test]
fn isolated_packets_need_no_allocation() {
    let mut map = PacketMap::new();
    for pn in 0..1001u64 {
        map.insert(pn, pn).unwrap();
    }
    for pn in (1..1000).step_by(2) {
        map.remove(pn);
    }
    check_invariants(&map);
    // Every run but the last holds one value inline.
    assert_eq!(map.runs.len(), 501);
    assert!(
        map.runs
            .values()
            .rev()
            .skip(1)
            .all(|span| span.values.capacity() == 1)
    );
}

/// A long run is emptied from the front, as when packets are acknowledged in order.
#[test]
fn drained_run_releases_memory() {
    let mut map = PacketMap::new();
    for pn in 0..10_000u64 {
        map.insert(pn, pn).unwrap();
    }
    for pn in 0..9_990 {
        map.remove(pn);
    }
    check_invariants(&map);
    assert!(capacity(&map) <= 40);
}

#[test]
fn packet_numbers_near_u64_max() {
    let mut map = PacketMap::new();
    for pn in u64::MAX - 4..=u64::MAX {
        map.insert(pn, pn).unwrap();
    }
    assert_eq!(map.get(u64::MAX), Some(&u64::MAX));
    assert_eq!(map.remove(u64::MAX - 2), Some(u64::MAX - 2));
    // A half-open range cannot include `u64::MAX` itself.
    let range: Vec<_> = map
        .iter_range(u64::MAX - 2..u64::MAX)
        .map(|(pn, _)| pn)
        .collect();
    assert_eq!(range, [u64::MAX - 1]);
    let all: Vec<_> = map.iter().map(|(pn, _)| pn).collect();
    assert_eq!(all, [u64::MAX - 4, u64::MAX - 3, u64::MAX - 1, u64::MAX]);
    check_invariants(&map);
}

#[test]
fn insert_at_or_below_last_fails() {
    let mut map = PacketMap::new();
    map.insert(10, 10u64).unwrap();
    map.insert(11, 11).unwrap();
    // Leave the run keyed below its first packet number.
    map.remove(10);
    for pn in [5, 10, 11] {
        assert_eq!(map.insert(pn, 0), Err(()));
    }
    check_invariants(&map);
    assert_eq!(entries(&map), [(11, 11)]);
}
