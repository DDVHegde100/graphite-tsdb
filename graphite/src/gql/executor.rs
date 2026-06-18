//! GQL query executor with predicate pushdown, column projection, SIMD filtering.

use super::ast::*;
use graphite_core::{Column, LsmTree, Tick};
use thiserror::Error;

#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;

#[derive(Error, Debug)]
pub enum ExecError {
    #[error("LSM error: {0}")]
    Lsm(#[from] graphite_core::LsmError),
    #[error("Execution error: {0}")]
    Message(String),
}

/// Aggregated OHLCV bar.
#[derive(Debug, Clone)]
pub struct OhlcvBar {
    pub timestamp: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: u64,
}

/// Query result row.
#[derive(Debug, Clone)]
pub struct ResultRow {
    pub timestamp: i64,
    pub symbol: String,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: u64,
}

/// Query execution result.
#[derive(Debug)]
pub struct QueryResult {
    pub rows: Vec<ResultRow>,
    pub explain_plan: Option<String>,
}

pub struct Executor<'a> {
    lsm: &'a LsmTree,
}

impl<'a> Executor<'a> {
    pub fn new(lsm: &'a LsmTree) -> Self {
        Self { lsm }
    }

    pub fn execute(&self, query: &Query) -> Result<QueryResult, ExecError> {
        if query.explain {
            let plan = self.build_explain_plan(query);
            return Ok(QueryResult {
                rows: Vec::new(),
                explain_plan: Some(plan.format(0)),
            });
        }

        let columns = match &query.columns {
            SelectColumns::All => Column::all().to_vec(),
            SelectColumns::Columns(cols) => cols.clone(),
        };

        let ticks = self.lsm.scan(
            Some(&query.symbol),
            query.where_clause.t1,
            query.where_clause.t2,
            &columns,
        )?;

        let filtered = self.apply_price_filter(&ticks, &query.where_clause.price_predicate);

        let rows = if let Some(group_by) = &query.group_by {
            self.aggregate(&filtered, group_by, &query.symbol)
        } else {
            filtered
                .iter()
                .map(|t| self.tick_to_row(t, &query.symbol))
                .collect()
        };

        let final_rows = if let Some(limit) = query.limit {
            rows.into_iter().take(limit as usize).collect()
        } else {
            rows
        };

        Ok(QueryResult {
            rows: final_rows,
            explain_plan: None,
        })
    }

    fn build_explain_plan(&self, query: &Query) -> ExplainNode {
        let stats = self.lsm.stats();
        let est_rows = stats.total_rows / 10; // rough estimate

        let scan = ExplainNode {
            operator: format!("SSTableScan(symbol={})", query.symbol),
            estimated_rows: est_rows,
            children: vec![ExplainNode {
                operator: format!(
                    "BloomFilterPushdown(t1={}, t2={})",
                    query.where_clause.t1, query.where_clause.t2
                ),
                estimated_rows: est_rows,
                children: vec![ExplainNode {
                    operator: "MemTableScan".into(),
                    estimated_rows: self.lsm.stats().total_rows.min(10000),
                    children: vec![],
                }],
            }],
        };

        let mut root = scan;

        if query.where_clause.price_predicate.is_some() {
            root = ExplainNode {
                operator: "SimdPriceFilter(AVX2)".into(),
                estimated_rows: est_rows / 2,
                children: vec![root],
            };
        }

        if let Some(gb) = &query.group_by {
            root = ExplainNode {
                operator: format!(
                    "StreamAggregate(interval={:?}, fn={:?})",
                    gb.interval, gb.aggregate
                ),
                estimated_rows: est_rows / 100,
                children: vec![root],
            };
        }

        if query.limit.is_some() {
            root = ExplainNode {
                operator: format!("Limit({})", query.limit.unwrap()),
                estimated_rows: query.limit.unwrap(),
                children: vec![root],
            };
        }

        ExplainNode {
            operator: "QueryRoot".into(),
            estimated_rows: root.estimated_rows,
            children: vec![root],
        }
    }

    fn apply_price_filter(&self, ticks: &[Tick], predicate: &Option<PricePredicate>) -> Vec<Tick> {
        match predicate {
            None => ticks.to_vec(),
            Some(PricePredicate::Greater(threshold)) => {
                simd_filter_prices(ticks, *threshold, SimdOp::Greater)
            }
            Some(PricePredicate::Less(threshold)) => {
                simd_filter_prices(ticks, *threshold, SimdOp::Less)
            }
            Some(PricePredicate::GreaterEq(threshold)) => ticks
                .iter()
                .filter(|t| t.close >= *threshold)
                .copied()
                .collect(),
            Some(PricePredicate::LessEq(threshold)) => ticks
                .iter()
                .filter(|t| t.close <= *threshold)
                .copied()
                .collect(),
            Some(PricePredicate::Equal(threshold)) => ticks
                .iter()
                .filter(|t| t.close == *threshold)
                .copied()
                .collect(),
        }
    }

    fn aggregate(&self, ticks: &[Tick], group_by: &GroupByClause, symbol: &str) -> Vec<ResultRow> {
        let interval_ns = group_by.interval.nanos();
        let mut bars: Vec<OhlcvBar> = Vec::new();

        for tick in ticks {
            let bucket = tick.timestamp / interval_ns * interval_ns;
            if let Some(bar) = bars.last_mut() {
                if bar.timestamp == bucket {
                    bar.high = bar.high.max(tick.high);
                    bar.low = bar.low.min(tick.low);
                    bar.close = tick.close;
                    bar.volume += tick.volume;
                    continue;
                }
            }
            bars.push(OhlcvBar {
                timestamp: bucket,
                open: tick.open,
                high: tick.high,
                low: tick.low,
                close: tick.close,
                volume: tick.volume,
            });
        }

        match group_by.aggregate {
            AggregateFn::Ohlcv => bars
                .iter()
                .map(|b| ResultRow {
                    timestamp: b.timestamp,
                    symbol: symbol.to_string(),
                    open: b.open,
                    high: b.high,
                    low: b.low,
                    close: b.close,
                    volume: b.volume,
                })
                .collect(),
            AggregateFn::Sum => bars
                .iter()
                .map(|b| ResultRow {
                    timestamp: b.timestamp,
                    symbol: symbol.to_string(),
                    open: 0.0,
                    high: 0.0,
                    low: 0.0,
                    close: b.volume as f64,
                    volume: b.volume,
                })
                .collect(),
            AggregateFn::Count => bars
                .iter()
                .map(|b| ResultRow {
                    timestamp: b.timestamp,
                    symbol: symbol.to_string(),
                    open: 0.0,
                    high: 0.0,
                    low: 0.0,
                    close: 1.0,
                    volume: 1,
                })
                .collect(),
            AggregateFn::Vwap => {
                let mut total_pv = 0.0;
                let mut total_vol = 0u64;
                for tick in ticks {
                    total_pv += tick.close * tick.volume as f64;
                    total_vol += tick.volume;
                }
                let vwap = if total_vol > 0 {
                    total_pv / total_vol as f64
                } else {
                    0.0
                };
                vec![ResultRow {
                    timestamp: ticks.first().map(|t| t.timestamp).unwrap_or(0),
                    symbol: symbol.to_string(),
                    open: vwap,
                    high: vwap,
                    low: vwap,
                    close: vwap,
                    volume: total_vol,
                }]
            }
        }
    }

    fn tick_to_row(&self, tick: &Tick, symbol: &str) -> ResultRow {
        ResultRow {
            timestamp: tick.timestamp,
            symbol: symbol.to_string(),
            open: tick.open,
            high: tick.high,
            low: tick.low,
            close: tick.close,
            volume: tick.volume,
        }
    }
}

enum SimdOp {
    Greater,
    Less,
}

/// Vectorized price filter using AVX2 when available.
fn simd_filter_prices(ticks: &[Tick], threshold: f64, op: SimdOp) -> Vec<Tick> {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") {
            return simd_filter_avx2(ticks, threshold, op);
        }
    }
    scalar_filter_prices(ticks, threshold, op)
}

#[cfg(target_arch = "x86_64")]
fn simd_filter_avx2(ticks: &[Tick], threshold: f64, op: SimdOp) -> Vec<Tick> {
    let mut result = Vec::new();
    let threshold_vec = _mm256_set1_pd(threshold);
    let mut i = 0;

    while i + 4 <= ticks.len() {
        let prices = [
            ticks[i].close,
            ticks[i + 1].close,
            ticks[i + 2].close,
            ticks[i + 3].close,
        ];
        let price_vec = _mm256_loadu_pd(prices.as_ptr());

        let mask = match op {
            SimdOp::Greater => _mm256_cmp_pd(price_vec, threshold_vec, _CMP_GT_OQ),
            SimdOp::Less => _mm256_cmp_pd(price_vec, threshold_vec, _CMP_LT_OQ),
        };

        let mask_bits = _mm256_movemask_pd(mask);
        for j in 0..4 {
            if mask_bits & (1 << j) != 0 {
                result.push(ticks[i + j]);
            }
        }
        i += 4;
    }

    for tick in &ticks[i..] {
        let passes = match op {
            SimdOp::Greater => tick.close > threshold,
            SimdOp::Less => tick.close < threshold,
        };
        if passes {
            result.push(*tick);
        }
    }

    result
}

fn scalar_filter_prices(ticks: &[Tick], threshold: f64, op: SimdOp) -> Vec<Tick> {
    ticks
        .iter()
        .filter(|t| match op {
            SimdOp::Greater => t.close > threshold,
            SimdOp::Less => t.close < threshold,
        })
        .copied()
        .collect()
}
