//! Python bindings for Graphite TSDB via PyO3.

use graphite::{DB, DbError, QueryResult};
use graphite_core::TickBatch;
use numpy::{PyReadonlyArray1, PyUntypedArrayMethods};
use pyo3::exceptions::{PyIOError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyDict;

fn db_err(e: DbError) -> PyErr {
    match e {
        DbError::Lsm(e) => PyIOError::new_err(e.to_string()),
        DbError::Parse(e) => PyValueError::new_err(e.to_string()),
        DbError::Exec(e) => PyRuntimeError::new_err(e.to_string()),
        DbError::NotOpen => PyRuntimeError::new_err("database not open"),
    }
}

/// Graphite time-series database for financial tick data.
#[pyclass(name = "DB")]
struct PyDB {
    inner: DB,
}

#[pymethods]
impl PyDB {
    #[staticmethod]
    fn open(path: &str) -> PyResult<Self> {
        let db = DB::open(path).map_err(db_err)?;
        Ok(PyDB { inner: db })
    }

    fn insert(
        &self,
        symbol: &str,
        timestamp_ns: i64,
        open: f64,
        high: f64,
        low: f64,
        close: f64,
        volume: u64,
    ) -> PyResult<()> {
        self.inner
            .insert(symbol, timestamp_ns, open, high, low, close, volume)
            .map_err(db_err)
    }

    /// Bulk insert from numpy arrays (single WAL fsync).
    fn insert_numpy(
        &self,
        symbol: &str,
        timestamps: PyReadonlyArray1<'_, i64>,
        opens: PyReadonlyArray1<'_, f64>,
        highs: PyReadonlyArray1<'_, f64>,
        lows: PyReadonlyArray1<'_, f64>,
        closes: PyReadonlyArray1<'_, f64>,
        volumes: PyReadonlyArray1<'_, u64>,
    ) -> PyResult<()> {
        let batch = TickBatch {
            timestamps: timestamps.as_slice()?.to_vec(),
            opens: opens.as_slice()?.to_vec(),
            highs: highs.as_slice()?.to_vec(),
            lows: lows.as_slice()?.to_vec(),
            closes: closes.as_slice()?.to_vec(),
            volumes: volumes.as_slice()?.to_vec(),
        };
        let n = batch.len();
        if batch.opens.len() != n
            || batch.highs.len() != n
            || batch.lows.len() != n
            || batch.closes.len() != n
            || batch.volumes.len() != n
        {
            return Err(PyValueError::new_err("all arrays must have same length"));
        }
        self.inner.insert_batch_columns(symbol, &batch).map_err(db_err)
    }

    /// Execute GQL and return a Polars DataFrame (falls back to dict if polars missing).
    fn query(&self, gql: &str, py: Python<'_>) -> PyResult<PyObject> {
        let result = self.inner.query(gql).map_err(db_err)?;
        result_to_polars_or_dict(py, &result)
    }

    /// Range query returning a Polars DataFrame.
    fn query_range(&self, symbol: &str, t1: i64, t2: i64, py: Python<'_>) -> PyResult<PyObject> {
        let result = self.inner.query_range(symbol, t1, t2).map_err(db_err)?;
        result_to_polars_or_dict(py, &result)
    }

    fn compact(&self) -> PyResult<()> {
        self.inner.compact().map_err(db_err)
    }

    fn needs_compaction(&self) -> bool {
        self.inner.needs_compaction()
    }

    fn stats(&self, py: Python<'_>) -> PyResult<PyObject> {
        let stats = self.inner.stats();
        let dict = PyDict::new_bound(py);
        dict.set_item("level_sizes", stats.level_sizes)?;
        dict.set_item("bloom_filter_hit_rate", stats.bloom_filter_hit_rate)?;
        dict.set_item("cache_hit_rate", stats.cache_hit_rate)?;
        dict.set_item("write_amplification_factor", stats.write_amplification_factor)?;
        dict.set_item("total_rows", stats.total_rows)?;
        dict.set_item("total_sstables", stats.total_sstables)?;
        Ok(dict.into())
    }
}

fn result_to_polars_or_dict(py: Python<'_>, result: &QueryResult) -> PyResult<PyObject> {
    if let Some(plan) = &result.explain_plan {
        let dict = PyDict::new_bound(py);
        dict.set_item("explain_plan", plan)?;
        dict.set_item("row_count", 0)?;
        return Ok(dict.into());
    }

  if let Ok(polars) = py.import_bound("polars") {
        let timestamps: Vec<i64> = result.rows.iter().map(|r| r.timestamp).collect();
        let symbols: Vec<String> = result.rows.iter().map(|r| r.symbol.clone()).collect();
        let opens: Vec<f64> = result.rows.iter().map(|r| r.open).collect();
        let highs: Vec<f64> = result.rows.iter().map(|r| r.high).collect();
        let lows: Vec<f64> = result.rows.iter().map(|r| r.low).collect();
        let closes: Vec<f64> = result.rows.iter().map(|r| r.close).collect();
        let volumes: Vec<u64> = result.rows.iter().map(|r| r.volume).collect();

        let kwargs = PyDict::new_bound(py);
        kwargs.set_item("timestamp", timestamps)?;
        kwargs.set_item("symbol", symbols)?;
        kwargs.set_item("open", opens)?;
        kwargs.set_item("high", highs)?;
        kwargs.set_item("low", lows)?;
        kwargs.set_item("close", closes)?;
        kwargs.set_item("volume", volumes)?;

        let df = polars.call_method("DataFrame", (), Some(&kwargs))?;
        return Ok(df.into());
    }

    result_to_dict(py, result)
}

fn result_to_dict(py: Python<'_>, result: &QueryResult) -> PyResult<PyObject> {
    let dict = PyDict::new_bound(py);
    let timestamps: Vec<i64> = result.rows.iter().map(|r| r.timestamp).collect();
    let symbols: Vec<&str> = result.rows.iter().map(|r| r.symbol.as_str()).collect();
    let opens: Vec<f64> = result.rows.iter().map(|r| r.open).collect();
    let highs: Vec<f64> = result.rows.iter().map(|r| r.high).collect();
    let lows: Vec<f64> = result.rows.iter().map(|r| r.low).collect();
    let closes: Vec<f64> = result.rows.iter().map(|r| r.close).collect();
    let volumes: Vec<u64> = result.rows.iter().map(|r| r.volume).collect();

    dict.set_item("timestamp", timestamps)?;
    dict.set_item("symbol", symbols)?;
    dict.set_item("open", opens)?;
    dict.set_item("high", highs)?;
    dict.set_item("low", lows)?;
    dict.set_item("close", closes)?;
    dict.set_item("volume", volumes)?;
    dict.set_item("row_count", result.rows.len())?;
    Ok(dict.into())
}

/// Graphite TSDB Python module.
#[pymodule]
fn graphite_tsdb(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyDB>()?;
    Ok(())
}
