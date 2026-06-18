# Changelog

## [0.3.0] - 2026-06-18

### Added
- `ScanStream` iterator — lazy SSTable loading without full range materialization
- `DB::scan_stream`, `DB::count_range` for streaming scans
- Cross-DB `compare_write` binary (Graphite + optional DuckDB/Influx/Timescale)
- `SharedBlockCache` clone support for scan iterators

## [0.2.0] - 2026-06-18

### Added
- WAL `append_batch` — bulk writes with a single `fsync`
- `TickBatch` and `SymbolTick` types for columnar / multi-symbol bulk insert
- `DB::insert_batch_columns` and `DB::insert_multi`
- Background compaction scheduler (tokio thread, configurable interval)
- ZSTD volume compression for L1+ SSTables (LZ4 remains default for L0)
- Python `query` / `query_range` return Polars `DataFrame` when polars is installed
- `insert_numpy` uses batched WAL append
- `DB::needs_compaction()` API
- Benchmark harness helpers for cross-DB comparison output

### Changed
- `LsmTree::open` returns `Arc<LsmTree>` for background compaction
- `LsmConfig` defaults: `auto_compact = true`, `compact_interval_ms = 5000`

## [0.1.0] - 2026-06-18

Initial release: custom LSM-tree, GQL, Python bindings, criterion benchmarks.
