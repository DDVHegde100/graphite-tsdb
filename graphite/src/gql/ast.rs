//! GQL AST nodes.

use graphite_core::Column;

#[derive(Debug, Clone, PartialEq)]
pub enum AggregateFn {
    Ohlcv,
    Sum,
    Count,
    Vwap,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Interval {
    Sec1,
    Min1,
    Hour1,
}

impl Interval {
    pub fn nanos(&self) -> i64 {
        match self {
            Interval::Sec1 => 1_000_000_000,
            Interval::Min1 => 60_000_000_000,
            Interval::Hour1 => 3_600_000_000_000,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum PricePredicate {
    Greater(f64),
    Less(f64),
    GreaterEq(f64),
    LessEq(f64),
    Equal(f64),
}

#[derive(Debug, Clone, PartialEq)]
pub struct WhereClause {
    pub t1: i64,
    pub t2: i64,
    pub price_predicate: Option<PricePredicate>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct GroupByClause {
    pub interval: Interval,
    pub aggregate: AggregateFn,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SelectColumns {
    All,
    Columns(Vec<Column>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Query {
    pub explain: bool,
    pub columns: SelectColumns,
    pub symbol: String,
    pub where_clause: WhereClause,
    pub limit: Option<u64>,
    pub group_by: Option<GroupByClause>,
}

/// Explain plan operator node.
#[derive(Debug, Clone)]
pub struct ExplainNode {
    pub operator: String,
    pub estimated_rows: u64,
    pub children: Vec<ExplainNode>,
}

impl ExplainNode {
    pub fn format(&self, indent: usize) -> String {
        let prefix = "  ".repeat(indent);
        let mut s = format!(
            "{}{} (est. {} rows)\n",
            prefix,
            self.operator,
            self.estimated_rows
        );
        for child in &self.children {
            s.push_str(&child.format(indent + 1));
        }
        s
    }
}
