//! Graphite CLI — embeddable TSDB shell tool.

use clap::{Parser, Subcommand};
use graphite::DB;
use std::path::PathBuf;
use std::process::ExitCode;

#[derive(Parser)]
#[command(name = "graphite", about = "Graphite time-series database CLI")]
struct Cli {
    /// Database directory path
    #[arg(short, long, default_value = "./graphite-data")]
    db: PathBuf,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Insert a single OHLCV tick
    Insert {
        symbol: String,
        #[arg(long)]
        timestamp: i64,
        #[arg(long, default_value_t = 0.0)]
        open: f64,
        #[arg(long, default_value_t = 0.0)]
        high: f64,
        #[arg(long, default_value_t = 0.0)]
        low: f64,
        #[arg(long, default_value_t = 0.0)]
        close: f64,
        #[arg(long, default_value_t = 0)]
        volume: u64,
    },
    /// Run a GQL query
    Query {
        gql: String,
        /// Output as JSON
        #[arg(long)]
        json: bool,
    },
    /// Range scan shortcut
    Range {
        symbol: String,
        #[arg(long)]
        from: i64,
        #[arg(long)]
        to: i64,
        #[arg(long)]
        json: bool,
    },
    /// Print database statistics
    Stats,
    /// Run compaction
    Compact,
    /// Count ticks in a range (streaming)
    Count {
        symbol: String,
        #[arg(long)]
        from: i64,
        #[arg(long)]
        to: i64,
    },
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let db = match DB::open(&cli.db) {
        Ok(db) => db,
        Err(e) => {
            eprintln!("error: failed to open database: {e}");
            return ExitCode::FAILURE;
        }
    };

    match cli.command {
        Commands::Insert {
            symbol,
            timestamp,
            open,
            high,
            low,
            close,
            volume,
        } => {
            if let Err(e) = db.insert(&symbol, timestamp, open, high, low, close, volume) {
                eprintln!("error: insert failed: {e}");
                return ExitCode::FAILURE;
            }
            println!("ok");
        }
        Commands::Query { gql, json } => {
            match db.query(&gql) {
                Ok(result) => {
                    if let Some(plan) = &result.explain_plan {
                        println!("{}", plan);
                    } else if json {
                        print_json_rows(&result.rows);
                    } else {
                        print_rows(&result.rows);
                    }
                }
                Err(e) => {
                    eprintln!("error: query failed: {e}");
                    return ExitCode::FAILURE;
                }
            }
        }
        Commands::Range {
            symbol,
            from,
            to,
            json,
        } => match db.query_range(&symbol, from, to) {
            Ok(result) => {
                if json {
                    print_json_rows(&result.rows);
                } else {
                    print_rows(&result.rows);
                }
            }
            Err(e) => {
                eprintln!("error: range query failed: {e}");
                return ExitCode::FAILURE;
            }
        },
        Commands::Stats => {
            let stats = db.stats();
            println!("{}", serde_json::to_string_pretty(&stats).unwrap_or_default());
        }
        Commands::Compact => {
            if let Err(e) = db.compact() {
                eprintln!("error: compact failed: {e}");
                return ExitCode::FAILURE;
            }
            println!("compaction complete");
        }
        Commands::Count {
            symbol,
            from,
            to,
        } => match db.count_range(&symbol, from, to) {
            Ok(n) => println!("{}", n),
            Err(e) => {
                eprintln!("error: count failed: {e}");
                return ExitCode::FAILURE;
            }
        },
    }

    ExitCode::SUCCESS
}

fn print_rows(rows: &[graphite::ResultRow]) {
    println!("{} rows", rows.len());
    for row in rows.iter().take(1000) {
        println!(
            "{} ts={} o={} h={} l={} c={} v={}",
            row.symbol, row.timestamp, row.open, row.high, row.low, row.close, row.volume
        );
    }
    if rows.len() > 1000 {
        println!("... {} more rows", rows.len() - 1000);
    }
}

fn print_json_rows(rows: &[graphite::ResultRow]) {
    let json_rows: Vec<_> = rows
        .iter()
        .map(|r| {
            serde_json::json!({
                "symbol": r.symbol,
                "timestamp": r.timestamp,
                "open": r.open,
                "high": r.high,
                "low": r.low,
                "close": r.close,
                "volume": r.volume,
            })
        })
        .collect();
    println!("{}", serde_json::to_string_pretty(&json_rows).unwrap_or_default());
}
