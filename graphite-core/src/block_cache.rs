//! LRU block cache using intrusive doubly-linked list + HashMap.

use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

/// Cache key: (sstable_id, block_offset).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CacheKey {
    pub sstable_id: u64,
    pub block_offset: u64,
}

/// Intrusive doubly-linked list node.
struct Node {
    key: CacheKey,
    data: Vec<u8>,
    prev: Option<usize>,
    next: Option<usize>,
}

/// LRU block cache with configurable capacity in bytes.
pub struct BlockCache {
    capacity: usize,
    current_size: usize,
    nodes: Vec<Node>,
    map: HashMap<CacheKey, usize>,
    head: Option<usize>,
    tail: Option<usize>,
    free_list: Vec<usize>,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl BlockCache {
    pub fn new(capacity_mb: usize) -> Self {
        Self {
            capacity: capacity_mb * 1024 * 1024,
            current_size: 0,
            nodes: Vec::new(),
            map: HashMap::new(),
            head: None,
            tail: None,
            free_list: Vec::new(),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    pub fn get(&self, key: &CacheKey) -> Option<Vec<u8>> {
        if let Some(&idx) = self.map.get(key) {
            self.hits.fetch_add(1, Ordering::Relaxed);
            Some(self.nodes[idx].data.clone())
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    pub fn insert(&mut self, key: CacheKey, data: Vec<u8>) {
        let data_size = data.len();
        if data_size > self.capacity {
            return;
        }

        if let Some(&idx) = self.map.get(&key) {
            self.current_size -= self.nodes[idx].data.len();
            self.nodes[idx].data = data;
            self.current_size += data_size;
            self.move_to_head(idx);
            self.evict_if_needed();
            return;
        }

        let idx = self.alloc_node(key, data);
        self.map.insert(key, idx);
        self.current_size += data_size;
        self.add_to_head(idx);
        self.evict_if_needed();
    }

    fn alloc_node(&mut self, key: CacheKey, data: Vec<u8>) -> usize {
        if let Some(idx) = self.free_list.pop() {
            self.nodes[idx] = Node {
                key,
                data,
                prev: None,
                next: None,
            };
            idx
        } else {
            let idx = self.nodes.len();
            self.nodes.push(Node {
                key,
                data,
                prev: None,
                next: None,
            });
            idx
        }
    }

    fn add_to_head(&mut self, idx: usize) {
        self.nodes[idx].prev = None;
        self.nodes[idx].next = self.head;

        if let Some(h) = self.head {
            self.nodes[h].prev = Some(idx);
        }
        self.head = Some(idx);

        if self.tail.is_none() {
            self.tail = Some(idx);
        }
    }

    fn move_to_head(&mut self, idx: usize) {
        if self.head == Some(idx) {
            return;
        }

        let prev = self.nodes[idx].prev;
        let next = self.nodes[idx].next;

        if let Some(p) = prev {
            self.nodes[p].next = next;
        }
        if let Some(n) = next {
            self.nodes[n].prev = prev;
        }
        if self.tail == Some(idx) {
            self.tail = prev;
        }

        self.nodes[idx].prev = None;
        self.nodes[idx].next = self.head;
        if let Some(h) = self.head {
            self.nodes[h].prev = Some(idx);
        }
        self.head = Some(idx);
    }

    fn remove_tail(&mut self) -> Option<usize> {
        let tail_idx = self.tail?;
        let prev = self.nodes[tail_idx].prev;

        self.tail = prev;
        if let Some(p) = prev {
            self.nodes[p].next = None;
        } else {
            self.head = None;
        }

        let key = self.nodes[tail_idx].key;
        self.current_size -= self.nodes[tail_idx].data.len();
        self.map.remove(&key);
        self.free_list.push(tail_idx);

        Some(tail_idx)
    }

    fn evict_if_needed(&mut self) {
        while self.current_size > self.capacity {
            self.remove_tail();
        }
    }

    pub fn hit_rate(&self) -> f64 {
        let hits = self.hits.load(Ordering::Relaxed);
        let misses = self.misses.load(Ordering::Relaxed);
        let total = hits + misses;
        if total == 0 {
            0.0
        } else {
            hits as f64 / total as f64
        }
    }

    pub fn clear(&mut self) {
        self.nodes.clear();
        self.map.clear();
        self.head = None;
        self.tail = None;
        self.free_list.clear();
        self.current_size = 0;
    }
}

/// Thread-safe wrapper for concurrent access.
pub struct SharedBlockCache {
    inner: Mutex<BlockCache>,
}

impl SharedBlockCache {
    pub fn new(capacity_mb: usize) -> Self {
        Self {
            inner: Mutex::new(BlockCache::new(capacity_mb)),
        }
    }

    pub fn get(&self, key: &CacheKey) -> Option<Vec<u8>> {
        self.inner.lock().get(key)
    }

    pub fn insert(&self, key: CacheKey, data: Vec<u8>) {
        self.inner.lock().insert(key, data);
    }

    pub fn hit_rate(&self) -> f64 {
        self.inner.lock().hit_rate()
    }
}
