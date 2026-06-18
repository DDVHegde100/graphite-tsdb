//! Graphite tick ingestion server — HTTP POST and WebSocket.

use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use clap::Parser;
use futures_util::{SinkExt, StreamExt};
use graphite::DB;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::info;

#[derive(Parser)]
#[command(name = "graphite-server", about = "Graphite tick ingestion server")]
struct Args {
    /// Database directory
    #[arg(short, long, default_value = "./graphite-data")]
    db: PathBuf,

    /// Listen address
    #[arg(short, long, default_value = "127.0.0.1:8080")]
    listen: SocketAddr,
}

#[derive(Clone)]
struct AppState {
    db: Arc<DB>,
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
    total_rows: u64,
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
    let db = Arc::new(DB::open(&args.db)?);
    let state = AppState { db };

    let app = Router::new()
        .route("/health", get(health))
        .route("/tick", post(insert_tick))
        .route("/ws", get(ws_handler))
        .with_state(state);

    info!("graphite-server listening on {}", args.listen);
    let listener = tokio::net::TcpListener::bind(args.listen).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn health(State(state): State<AppState>) -> Json<HealthResponse> {
    let stats = state.db.stats();
    Json(HealthResponse {
        status: "ok",
        total_rows: stats.total_rows,
    })
}

async fn insert_tick(
    State(state): State<AppState>,
    Json(payload): Json<TickPayload>,
) -> (StatusCode, Json<TickResponse>) {
    match insert_payload(&state.db, &payload) {
        Ok(()) => (
            StatusCode::OK,
            Json(TickResponse {
                ok: true,
                error: None,
            }),
        ),
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
                let response = match serde_json::from_str::<TickPayload>(&text) {
                    Ok(payload) => match insert_payload(&state.db, &payload) {
                        Ok(()) => serde_json::json!({ "ok": true }),
                        Err(e) => serde_json::json!({ "ok": false, "error": e.to_string() }),
                    },
                    Err(e) => serde_json::json!({ "ok": false, "error": e.to_string() }),
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
