//! Probabilistic skip-list with O(log n) insert/lookup.
//! Used as the in-memory MemTable — no BTreeMap.

use crate::types::{Key, Tick};
use rand::Rng;
use std::cmp::Ordering;
use std::ptr;

const MAX_LEVEL: usize = 16;
const P: f64 = 0.25;

struct Node {
    key: Key,
    tick: Tick,
    forward: [*mut Node; MAX_LEVEL + 1],
}

impl Node {
    fn new(key: Key, tick: Tick, _level: usize) -> *mut Self {
        let node = Box::new(Node {
            key,
            tick,
            forward: [ptr::null_mut(); MAX_LEVEL + 1],
        });
        Box::into_raw(node)
    }
}

/// Skip-list MemTable with probabilistic level assignment.
pub struct SkipList {
    head: *mut Node,
    level: usize,
    len: usize,
}

impl SkipList {
    pub fn new() -> Self {
        let head = Node::new(
            Key::new(0, i64::MIN),
            Tick {
                symbol_id: 0,
                timestamp: i64::MIN,
                open: 0.0,
                high: 0.0,
                low: 0.0,
                close: 0.0,
                volume: 0,
            },
            MAX_LEVEL,
        );
        Self {
            head,
            level: 0,
            len: 0,
        }
    }

    fn random_level(&self) -> usize {
        let mut rng = rand::thread_rng();
        let mut lvl = 0;
        while rng.gen::<f64>() < P && lvl < MAX_LEVEL {
            lvl += 1;
        }
        lvl
    }

    #[allow(clippy::needless_range_loop)]
    pub fn insert(&mut self, tick: Tick) {
        let key = Key::new(tick.symbol_id, tick.timestamp);
        let new_level = self.random_level();
        let mut update: [*mut Node; MAX_LEVEL + 1] = [ptr::null_mut(); MAX_LEVEL + 1];

        let mut current = self.head;
        for i in (0..=self.level).rev() {
            loop {
                let next = unsafe { (*current).forward[i] };
                if next.is_null() {
                    break;
                }
                let next_key = unsafe { (*next).key };
                if next_key.cmp(&key) != Ordering::Less {
                    break;
                }
                current = next;
            }
            update[i] = current;
        }

        let next_at_0 = unsafe { (*current).forward[0] };
        if !next_at_0.is_null() {
            let next_key = unsafe { (*next_at_0).key };
            if next_key == key {
                unsafe {
                    (*next_at_0).tick = tick;
                }
                return;
            }
        }

        if new_level > self.level {
            for i in (self.level + 1)..=new_level {
                update[i] = self.head;
            }
            self.level = new_level;
        }

        let new_node = Node::new(key, tick, new_level);
        for i in 0..=new_level {
            unsafe {
                (*new_node).forward[i] = (*update[i]).forward[i];
                (*update[i]).forward[i] = new_node;
            }
        }
        self.len += 1;
    }

    pub fn get(&self, key: &Key) -> Option<Tick> {
        let mut current = self.head;
        for i in (0..=self.level).rev() {
            loop {
                let next = unsafe { (*current).forward[i] };
                if next.is_null() {
                    break;
                }
                let next_key = unsafe { (*next).key };
                if next_key.cmp(key) != Ordering::Less {
                    break;
                }
                current = next;
            }
        }

        let next = unsafe { (*current).forward[0] };
        if !next.is_null() {
            let next_key = unsafe { (*next).key };
            if next_key == *key {
                return Some(unsafe { (*next).tick });
            }
        }
        None
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn iter(&self) -> SkipListIter {
        SkipListIter {
            current: unsafe { (*self.head).forward[0] },
        }
    }

    pub fn drain(&mut self) -> Vec<Tick> {
        let mut result = Vec::with_capacity(self.len);
        let mut current = unsafe { (*self.head).forward[0] };
        while !current.is_null() {
            let tick = unsafe { (*current).tick };
            result.push(tick);
            current = unsafe { (*current).forward[0] };
        }
        self.clear();
        result
    }

    pub fn clear(&mut self) {
        let mut current = unsafe { (*self.head).forward[0] };
        while !current.is_null() {
            let next = unsafe { (*current).forward[0] };
            unsafe {
                drop(Box::from_raw(current));
            }
            current = next;
        }
        for i in 0..=self.level {
            unsafe {
                (*self.head).forward[i] = ptr::null_mut();
            }
        }
        self.level = 0;
        self.len = 0;
    }
}

impl Default for SkipList {
    fn default() -> Self {
        Self::new()
    }
}

pub struct SkipListIter {
    current: *mut Node,
}

impl Iterator for SkipListIter {
    type Item = Tick;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current.is_null() {
            return None;
        }
        let tick = unsafe { (*self.current).tick };
        self.current = unsafe { (*self.current).forward[0] };
        Some(tick)
    }
}

impl Drop for SkipList {
    fn drop(&mut self) {
        self.clear();
        unsafe {
            drop(Box::from_raw(self.head));
        }
    }
}

// Skip-list nodes are only accessed through RwLock guards.
unsafe impl Send for SkipList {}
unsafe impl Sync for SkipList {}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_tick(sym: u16, ts: i64, price: f64) -> Tick {
        Tick {
            symbol_id: sym,
            timestamp: ts,
            open: price,
            high: price + 1.0,
            low: price - 1.0,
            close: price,
            volume: 100,
        }
    }

    #[test]
    fn insert_and_lookup() {
        let mut sl = SkipList::new();
        sl.insert(make_tick(1, 1000, 50.0));
        sl.insert(make_tick(1, 2000, 51.0));
        sl.insert(make_tick(2, 1000, 100.0));

        let tick = sl.get(&Key::new(1, 2000)).unwrap();
        assert_eq!(tick.close, 51.0);

        assert!(sl.get(&Key::new(1, 3000)).is_none());
    }

    #[test]
    fn update_existing() {
        let mut sl = SkipList::new();
        sl.insert(make_tick(1, 1000, 50.0));
        sl.insert(make_tick(1, 1000, 55.0));
        assert_eq!(sl.len(), 1);
        let tick = sl.get(&Key::new(1, 1000)).unwrap();
        assert_eq!(tick.close, 55.0);
    }

    #[test]
    fn sorted_iteration() {
        let mut sl = SkipList::new();
        for i in 0..100 {
            sl.insert(make_tick(1, i as i64 * 1000, i as f64));
        }
        let ticks: Vec<_> = sl.iter().collect();
        assert_eq!(ticks.len(), 100);
        for i in 1..ticks.len() {
            assert!(ticks[i].timestamp > ticks[i - 1].timestamp);
        }
    }
}
