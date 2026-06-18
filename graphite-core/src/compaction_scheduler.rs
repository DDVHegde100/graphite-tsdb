//! Background compaction scheduler using tokio.

use crate::lsm::LsmTree;
use std::sync::Arc;
use std::time::Duration;

/// Spawn a background thread that runs compaction when the LSM-tree needs it.
pub fn spawn_background_compaction(lsm: Arc<LsmTree>, interval_ms: u64) {
    std::thread::Builder::new()
        .name("graphite-compact".into())
        .spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .build()
                .expect("graphite compaction runtime");

            rt.block_on(async move {
                let interval = Duration::from_millis(interval_ms.max(100));
                let mut ticker = tokio::time::interval(interval);
                loop {
                    ticker.tick().await;
                    if lsm.needs_compaction() {
                        let _ = lsm.compact();
                    }
                }
            });
        })
        .expect("spawn compaction thread");
}
