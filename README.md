# Graphite

**Embeddable time-series database for financial tick data.** Built in Rust from scratch — custom LSM-tree, columnar compression, and a SQL-inspired query language. Single binary at runtime, zero external services, embeds directly in Rust or Python applications.

```
  pip install graphite-tsdb          # Python
  cargo add graphite                 # Rust
```

---

## Why Graphite?

Most TSDBs are either **servers you deploy** (InfluxDB, TimescaleDB) or **wrappers around columnar formats** (DuckDB on Parquet). Graphite is different:

| | Graphite | Typical TSDB |
|---|---|---|
| **Deployment** | Embed in-process | Separate server process |
| **Runtime deps** | None (statically linked) | JVM, Postgres, HTTP stack |
| **Storage** | Custom LSM + columnar SSTables | Generic heap/B-tree or remote |
| **Tick schema** | Native OHLCV + nanosecond timestamps | Generic or bolted-on |
| **Query** | GQL with bloom pushdown + SIMD | SQL over network |

Designed for workloads like: market data replay, algo backtesting, embedded charting backends, and edge devices that need tick storage without running a database server.

---

## Architecture

```
                    ┌─────────────────────────────────────────┐
                    │              Application                │
                    │     Rust crate  ·  Python (PyO3)        │
                    └────────────────────┬────────────────────┘
                                         │
                    ┌────────────────────▼────────────────────┐
                    │              graphite (DB API)          │
                    │  insert · query · compact · stats       │
                    └────────────────────┬────────────────────┘
                                         │
              ┌──────────────────────────┼──────────────────────────┐
              │                          │                          │
    ┌─────────▼─────────┐    ┌───────────▼──────────┐    ┌─────────▼─────────┐
    │   GQL Parser      │    │   Query Executor     │    │   Symbol Dict     │
    │ recursive descent │───▶│ bloom pushdown       │    │ string → u16 ID   │
    │ EXPLAIN plans     │    │ column projection    │    └───────────────────┘
    └───────────────────┘    │ AVX2 price filters   │
                               └───────────┬──────────┘
                                           │
                    ┌──────────────────────▼──────────────────────┐
                    │           graphite-core (LSM-tree)            │
                    │                                             │
                    │  MemTable (skip-list)  ──flush──▶  SSTables │
                    │         │                              │    │
                    │         ▼                              │    │
                    │    WAL (CRC32 + fsync)            L0 → L1+  │
                    │                                     compaction│
                    │                              Block cache (LRU)│
                    └─────────────────────────────────────────────┘
```

---

## Features

### Storage engine (custom LSM-tree)

- **MemTable** — Probabilistic skip-list (`p = 0.25`), O(log n) insert/lookup. No `BTreeMap`.
- **WAL** — Binary append-only log, CRC32 per record, `fsync` on commit, full replay on crash recovery.
- **SSTables** — Immutable columnar files with:
  - 4 KB data pages, prefix-compressed keys
  - Sparse index every 16 keys
  - Per-table bloom filter (FNV-1a, ~1% false positive rate)
  - Metadata block (min/max timestamp, row count, level)
- **Compaction** — Leveled strategy: L0 (max 4 overlapping SSTables), L1+ (non-overlapping, 10× size ratio per level).
- **Block cache** — Configurable LRU (intrusive doubly-linked list + `HashMap`).

### Columnar compression

Each column is encoded separately inside SSTable data blocks for vectorized scan performance.

| Column | Encoding | Typical ratio |
|--------|----------|---------------|
| `timestamp` | Delta + bit-pack | ~2–4× vs raw i64 |
| `open/high/low/close` | Double-delta + Gorilla XOR | ~1.37 bits/value (Facebook Gorilla paper) |
| `volume` | RLE + LZ4 block | High on repeated lots |
| `symbol` | Dictionary (u16 ID) | ~2 bytes vs variable string |

### GQL — Graphite Query Language

Hand-written recursive descent parser (no `nom` / `pest`). Grammar:

```sql
EXPLAIN SELECT {columns | *}
FROM {symbol}
WHERE timestamp BETWEEN {t1} AND {t2}
  [AND price > {x}]
  [LIMIT n]
  [GROUP BY :{1s | 1m | 1h} AGGREGATE {OHLCV | SUM | COUNT | VWAP}]
```

**Executor optimizations:**
- Predicate pushdown to SSTable bloom filters (skip files that cannot contain the symbol)
- Column projection (decompress only requested columns)
- AVX2 vectorized `f64` price comparisons on x86_64 (`std::arch`)
- Streaming `GROUP BY` aggregation without materializing the full result set
- `EXPLAIN SELECT` outputs an operator tree with estimated row counts

### Python bindings

PyO3 + maturin, published as `graphite-tsdb`:

```python
import graphite
import numpy as np

db = graphite.open("./market-data")

# Single tick
db.insert("AAPL", 1700000000000000000, 150.0, 151.0, 149.0, 150.5, 10000)

# Bulk insert via numpy
n = 100_000
db.insert_numpy(
    "AAPL",
  timestamps=np.arange(n) * 1_000_000_000,
    opens=np.full(n, 150.0),
    highs=np.full(n, 151.0),
    lows=np.full(n, 149.0),
    closes=150.0 + np.arange(n) * 0.01,
    volumes=np.full(n, 1000, dtype=np.uint64),
)

# GQL query → columnar dict (polars-friendly)
result = db.query("SELECT close, volume FROM AAPL WHERE timestamp BETWEEN 0 AND 999000000000 AND price > 150.0")

# Range scan shortcut
df = db.query_range("AAPL", 0, 999_000_000_000)

# Maintenance
db.compact()
stats = db.stats()  # level sizes, bloom hit rate, cache hit rate, WAF
```

Type stubs: `graphite/graphite.pyi`

---

## Quick start (Rust)

```rust
use graphite::DB;

let db = DB::open("/tmp/graphite-data")?;

db.insert("AAPL", 1_700_000_000_000_000_000, 150.0, 151.0, 149.0, 150.5, 10000)?;

let result = db.query(
    "SELECT * FROM AAPL WHERE timestamp BETWEEN 0 AND 999000000000"
)?;
println!("{} rows", result.rows.len());

if let Some(tick) = db.get("AAPL", 1_700_000_000_000_000_000)? {
    println!("close = {}", tick.close);
}

db.compact()?;
let stats = db.stats();
println!("write amplification: {:.2}", stats.write_amplification_factor);
```

---

## Project layout

```
graphiite/
├── graphite-core/          # LSM-tree storage engine
│   └── src/
│       ├── skip_list.rs    # MemTable
│       ├── wal.rs          # Write-ahead log
│       ├── sstable.rs      # Columnar SSTable format
│       ├── compaction.rs   # Leveled compaction
│       ├── lsm.rs          # LSM-tree coordinator
│       ├── bloom.rs        # FNV-1a bloom filters
│       ├── block_cache.rs  # LRU block cache
│       └── compression/    # Delta, Gorilla, RLE+LZ4, dictionary
├── graphite/               # DB API + GQL
│   └── src/gql/            # Parser, AST, executor
├── graphite-py/            # Python bindings (PyO3)
└── graphite-bench/         # Criterion benchmarks
```

---

## Build & test

**Requirements:** Rust 1.70+ (tested on 1.90), Python 3.8–3.13 for bindings.

```bash
# Rust library
cargo build --release
cargo test --all

# Benchmarks (write throughput, point query, range scan)
cargo bench -p graphite-bench

# Python bindings (dev install)
pip install maturin
maturin develop -m graphite-py/pyproject.toml

# Python 3.14+: set before building
export PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1
```

---

## Benchmarks

Criterion suite in `graphite-bench`:

| Benchmark | What it measures |
|-----------|------------------|
| `write_throughput` | Sequential inserts (10K / 100K / 1M ticks), ops/sec |
| `point_query` | Random single-tick lookup by symbol + timestamp |
| `range_scan` | Full range scan over 100K and 1M rows |

```bash
cargo bench -p graphite-bench
# HTML reports: target/criterion/report/index.html
```

For comparisons against InfluxDB, DuckDB, or TimescaleDB, run the same tick workload against those systems separately — Graphite benchmarks use identical OHLCV schemas for fair comparison.

---

## Configuration

```rust
use graphite::{DB, LsmConfig};

let config = LsmConfig {
    cache_size_mb: 128,
    memtable_flush_threshold: 10000,
    auto_compact: true,
    compact_interval_ms: 5000,
};

let db = DB::open_with_config("./data", config)?;
```

---

## EXPLAIN example

```sql
EXPLAIN SELECT * FROM AAPL
WHERE timestamp BETWEEN 0 AND 1000000000000 AND price > 150.0
GROUP BY :1m AGGREGATE OHLCV
LIMIT 1000
```

```
QueryRoot (est. 100000 rows)
  Limit(1000) (est. 1000 rows)
    StreamAggregate(interval=Min1, fn=Ohlcv) (est. 1000 rows)
      SimdPriceFilter(AVX2) (est. 50000 rows)
        SSTableScan(symbol=AAPL) (est. 100000 rows)
          BloomFilterPushdown(t1=0, t2=1000000000000) (est. 100000 rows)
            MemTableScan (est. 10000 rows)
```

---

## Roadmap

- [x] Async compaction via tokio background task
- [x] Multi-symbol batch insert API
- [x] Native Polars DataFrame return in Python
- [x] ZSTD option for cold SSTable tiers (L1+ volumes)
- [ ] Cross-DB benchmark harness (InfluxDB, DuckDB, TimescaleDB)
- [ ] Streaming iterator API for large range scans

---

## License

MIT — see [LICENSE](LICENSE).
