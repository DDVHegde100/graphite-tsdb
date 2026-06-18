# Changelog

## [0.6.0] - 2026-06-18

### Added
- WAL streaming replication with primary/replica `NodeRole`
- `DB::open_replica`, `apply_replication_batch`, `read_wal_for_replication`
- Replication HTTP API on `graphite-server` (`/replication/wal`, `/replication/apply`, `/replication/status`)
- Primary push and replica pull sync (`--role`, `--primary-url`, `--replica-urls`)

## [0.5.0] - 2026-06-18

### Added
- S3 / object-store cold tier for archived SSTables (`cold-tier` feature, `LsmConfig::cold_tier_uri`)
- `DB::sync_cold_tier()` uploads L2+ SSTables to `s3://` or `file://` backends
- `graphite-server` — HTTP POST `/tick` and WebSocket `/ws` tick ingestion
- GitHub Actions Python 3.13 CI for `graphite-py`
- Release workflow for crates.io and PyPI on version tags

### Changed
- CI tests and clippy run with `cold-tier` feature enabled

## [0.4.0] - 2026-06-18

### Added
- `graphite-cli` — command-line insert, query, stats, compact, count
- GitHub Actions CI (test + clippy)
- Python `scan_iter` for streaming tick iteration
- Rust example at `graphite/examples/basic.rs`

### Changed
- Cleaned up compiler warnings in core storage engine

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
