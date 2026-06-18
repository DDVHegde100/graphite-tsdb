//! Graphite tick ingestion server — HTTP POST, WebSocket, and WAL replication.

use axum::{
    extract::{Query, State, ws::{Message, WebSocket, WebSocketUpgrade}},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use clap::Parser;
use futures_util::{SinkExt, StreamExt};
use graphite::DB;
use graphite_core::{NodeRole, ReplicationBatch, ReplicationEntry, ReplicationStatus};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tracing::info;

#[derive(Parser)]
#[command(name = "graphite-server", about = "Graphite tick ingestion and replication server")]
struct Args {
    /// Database directory
    #[arg(short, long, default_value = "./graphite-data")]
    db: PathBuf,

    /// Listen address
    #[arg(short, long, default_value = "127.0.0.1:8080")]
    listen: SocketAddr,

    /// Node role: primary accepts writes; replica is read-only
    #[arg(long, default_value = "primary")]
    role: String,

    /// Primary URL for replica sync (e.g. http://127.0.0.1:8080)
    #[arg(long)]
    primary_url: Option<String>,

    /// Replica URLs to push WAL entries (comma-separated, primary only)
    #[arg(long, value_delimiter = ',')]
    replica_urls: Vec<String>,

    /// Replica pull interval in milliseconds
    #[arg(long, default_value_t = 1000)]
    sync_interval_ms: u64,
}

const NEVER_PUSHED: u64 = u64::MAX;

#[derive(Clone)]
struct AppState {
    db: Arc<DB>,
    role: NodeRole,
    primary_url: Option<String>,
    replica_urls: Vec<String>,
    http: reqwest::Client,
    last_pushed_seq: Arc<AtomicU64>,
}

#[derive(Debug, Deserialize)]
struct TickPayload {
    symbol: String,
    timestamp: i64,
    #[serde(default)]
    open: f64,
    #[serde(default)]
    high: f64,
    #[serde(default)]
    low: f64,
    #[serde(default)]
    close: f64,
    #[serde(default)]
    volume: u64,
}

#[derive(Debug, Serialize)]
struct TickResponse {
    ok: bool,
    error: Option<String>,
}

#[derive(Debug, Serialize)]
struct HealthResponse {
    status: &'static str,
    role: NodeRole,
    total_rows: u64,
}

#[derive(Debug, Deserialize)]
struct WalQuery {
    /// Last WAL sequence applied on the replica (inclusive). Omit for full stream.
    after: Option<u64>,
    #[serde(default = "default_limit")]
    limit: usize,
}

fn default_limit() -> usize {
    500
}

#[derive(Debug, Serialize, Deserialize)]
struct WalStreamResponse {
    entries: Vec<ReplicationEntry>,
    wal_sequence: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();
    let role = parse_role(&args.role)?;

    let db = match role {
        NodeRole::Primary => Arc::new(DB::open(&args.db)?),
        NodeRole::Replica => Arc::new(DB::open_replica(&args.db, graphite_core::LsmConfig::default())?),
    };

    let state = AppState {
        db,
        role,
        primary_url: args.primary_url,
        replica_urls: args.replica_urls,
        http: reqwest::Client::new(),
        last_pushed_seq: Arc::new(AtomicU64::new(NEVER_PUSHED)),
    };

    if state.role == NodeRole::Replica {
        if state.primary_url.is_none() {
            return Err("replica requires --primary-url".into());
        }
        let sync_state = state.clone();
        let interval = Duration::from_millis(args.sync_interval_ms);
        tokio::spawn(async move {
            replica_sync_loop(sync_state, interval).await;
        });
    }

    let app = Router::new()
        .route("/health", get(health))
        .route("/tick", post(insert_tick))
        .route("/ws", get(ws_handler))
        .route("/replication/status", get(replication_status))
        .route("/replication/wal", get(replication_wal))
        .route("/replication/apply", post(replication_apply))
        .with_state(state);

    info!("graphite-server ({:?}) listening on {}", role, args.listen);
    let listener = tokio::net::TcpListener::bind(args.listen).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

fn parse_role(s: &str) -> Result<NodeRole, String> {
    match s.to_lowercase().as_str() {
        "primary" => Ok(NodeRole::Primary),
        "replica" => Ok(NodeRole::Replica),
        _ => Err(format!("invalid role: {s} (use primary or replica)")),
    }
}

async fn health(State(state): State<AppState>) -> Json<HealthResponse> {
    let stats = state.db.stats();
    Json(HealthResponse {
        status: "ok",
        role: state.role,
        total_rows: stats.total_rows,
    })
}

async fn replication_status(State(state): State<AppState>) -> Json<ReplicationStatus> {
    Json(state.db.replication_status())
}

async fn replication_wal(
    State(state): State<AppState>,
    Query(query): Query<WalQuery>,
) -> Result<Json<WalStreamResponse>, StatusCode> {
    if state.role != NodeRole::Primary {
        return Err(StatusCode::FORBIDDEN);
    }
    match state.db.read_wal_for_replication(query.after, query.limit) {
        Ok(entries) => {
            let wal_sequence = state.db.replication_status().wal_sequence;
            Ok(Json(WalStreamResponse {
                entries,
                wal_sequence,
            }))
        }
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

async fn replication_apply(
    State(state): State<AppState>,
    Json(batch): Json<ReplicationBatch>,
) -> Result<Json<TickResponse>, StatusCode> {
    if state.role != NodeRole::Replica {
        return Err(StatusCode::FORBIDDEN);
    }
    match state.db.apply_replication_batch(&batch.entries) {
        Ok(_) => Ok(Json(TickResponse {
            ok: true,
            error: None,
        })),
        Err(e) => Ok(Json(TickResponse {
            ok: false,
            error: Some(e.to_string()),
        })),
    }
}

async fn insert_tick(
    State(state): State<AppState>,
    Json(payload): Json<TickPayload>,
) -> (StatusCode, Json<TickResponse>) {
    if state.role != NodeRole::Primary {
        return (
            StatusCode::FORBIDDEN,
            Json(TickResponse {
                ok: false,
                error: Some("replica cannot accept writes".into()),
            }),
        );
    }
    match insert_payload(&state.db, &payload) {
        Ok(()) => {
            schedule_replication_push(state.clone());
            (
                StatusCode::OK,
                Json(TickResponse {
                    ok: true,
                    error: None,
                }),
            )
        }
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(TickResponse {
                ok: false,
                error: Some(e.to_string()),
            }),
        ),
    }
}

async fn ws_handler(ws: WebSocketUpgrade, State(state): State<AppState>) -> impl IntoResponse {
    ws.on_upgrade(|socket| handle_socket(socket, state))
}

async fn handle_socket(socket: WebSocket, state: AppState) {
    let (mut sender, mut receiver) = socket.split();

    while let Some(msg) = receiver.next().await {
        match msg {
            Ok(Message::Text(text)) => {
                let response = if state.role != NodeRole::Primary {
                    serde_json::json!({ "ok": false, "error": "replica cannot accept writes" })
                } else {
                    match serde_json::from_str::<TickPayload>(&text) {
                        Ok(payload) => match insert_payload(&state.db, &payload) {
                            Ok(()) => {
                                schedule_replication_push(state.clone());
                                serde_json::json!({ "ok": true })
                            }
                            Err(e) => serde_json::json!({ "ok": false, "error": e.to_string() }),
                        },
                        Err(e) => serde_json::json!({ "ok": false, "error": e.to_string() }),
                    }
                };
                if sender
                    .send(Message::Text(response.to_string()))
                    .await
                    .is_err()
                {
                    break;
                }
            }
            Ok(Message::Close(_)) | Err(_) => break,
            Ok(_) => {}
        }
    }
}

fn insert_payload(db: &DB, payload: &TickPayload) -> Result<(), graphite::DbError> {
    db.insert(
        &payload.symbol,
        payload.timestamp,
        payload.open,
        payload.high,
        payload.low,
        payload.close,
        payload.volume,
    )
}

fn schedule_replication_push(state: AppState) {
    if state.replica_urls.is_empty() || state.role != NodeRole::Primary {
        return;
    }
    tokio::spawn(async move {
        if let Err(e) = push_to_replicas(&state).await {
            tracing::warn!("replication push failed: {e}");
        }
    });
}

async fn push_to_replicas(state: &AppState) -> Result<(), Box<dyn std::error::Error>> {
    let pushed = state.last_pushed_seq.load(Ordering::Relaxed);
    let after = if pushed == NEVER_PUSHED {
        None
    } else {
        Some(pushed)
    };
    let entries = state.db.read_wal_for_replication(after, 500)?;
    if entries.is_empty() {
        return Ok(());
    }
    let max_seq = entries.last().map(|e| e.sequence).unwrap_or(pushed);
    let batch = ReplicationBatch { entries };

    for base in &state.replica_urls {
        let url = format!("{}/replication/apply", base.trim_end_matches('/'));
        state.http.post(&url).json(&batch).send().await?;
    }

    state.last_pushed_seq.store(max_seq, Ordering::Relaxed);
    Ok(())
}

async fn replica_sync_loop(state: AppState, interval: Duration) {
    let primary = state.primary_url.clone().unwrap();
    loop {
        tokio::time::sleep(interval).await;
        let after = state.db.replication_last_applied();
        let url = match after {
            None => format!("{}/replication/wal?limit=500", primary.trim_end_matches('/')),
            Some(seq) => format!(
                "{}/replication/wal?after={}&limit=500",
                primary.trim_end_matches('/'),
                seq
            ),
        };
        match state.http.get(&url).send().await {
            Ok(resp) if resp.status().is_success() => {
                if let Ok(body) = resp.json::<WalStreamResponse>().await {
                    if !body.entries.is_empty() {
                        if let Err(e) = state.db.apply_replication_batch(&body.entries) {
                            tracing::warn!("replica apply failed: {e}");
                        }
                    }
                }
            }
            Ok(resp) => tracing::warn!("primary wal fetch status: {}", resp.status()),
            Err(e) => tracing::warn!("primary wal fetch error: {e}"),
        }
    }
}
