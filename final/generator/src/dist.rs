use anyhow::{Context, Result};
use aws_sdk_s3::Client as S3Client;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;

use crate::AppConfig;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Distributions {
    pub sample_size:         usize,
    pub events_per_session:  Vec<u32>,
    pub event_kinds:         HashMap<String, f64>,
    pub inter_event_seconds: Vec<u32>,
    /// (doc_id, freq) — pre-sorted by descending frequency.
    pub doc_ids:             Vec<(String, u64)>,
    pub queries:             Vec<String>,
}

pub async fn load_or_snapshot(s3: &S3Client, cfg: &AppConfig) -> Result<Distributions> {
    let path = Path::new(&cfg.dist_cache_path);
    if path.exists() {
        let txt = std::fs::read_to_string(path)?;
        let d: Distributions = serde_json::from_str(&txt)?;
        tracing::info!("loaded cached distributions from {}", cfg.dist_cache_path);
        return Ok(d);
    }
    tracing::info!("snapshotting distributions from s3://{}/", cfg.s3_bucket);
    let d = snapshot_from_s3(s3, cfg).await?;
    if let Some(parent) = path.parent() { std::fs::create_dir_all(parent).ok(); }
    std::fs::write(path, serde_json::to_string(&d)?)?;
    Ok(d)
}

async fn snapshot_from_s3(s3: &S3Client, cfg: &AppConfig) -> Result<Distributions> {
    let mut continuation: Option<String> = None;
    let mut all_keys: Vec<String> = Vec::new();
    loop {
        let mut req = s3.list_objects_v2().bucket(&cfg.s3_bucket).max_keys(1000);
        if let Some(t) = &continuation { req = req.continuation_token(t); }
        let resp = req.send().await.context("list_objects_v2")?;
        if let Some(contents) = resp.contents {
            for o in contents { if let Some(k) = o.key { all_keys.push(k); } }
        }
        if resp.is_truncated.unwrap_or(false) {
            continuation = resp.next_continuation_token;
        } else { break; }
    }
    tracing::info!(total_keys = all_keys.len(), "listing complete");

    // Sample up to 500 files (sufficient for stable distribution estimates).
    let sample_n = all_keys.len().min(500);
    let mut events_per_session:  Vec<u32>            = Vec::new();
    let mut event_kinds:         HashMap<String,u64> = HashMap::new();
    let mut inter_event_seconds: Vec<u32>            = Vec::new();
    let mut doc_id_freq:         HashMap<String,u64> = HashMap::new();
    let mut queries:             Vec<String>         = Vec::new();

    for key in all_keys.iter().take(sample_n) {
        let obj = s3.get_object().bucket(&cfg.s3_bucket).key(key).send().await?;
        let body = obj.body.collect().await?.into_bytes();
        let (cow, _, _) = encoding_rs::WINDOWS_1251.decode(&body);
        let text = cow.into_owned();
        let stats = crate::session::scan_session(&text);
        events_per_session.push(stats.event_count);
        inter_event_seconds.extend(stats.gaps);
        for k in stats.kinds.iter() { *event_kinds.entry(k.clone()).or_insert(0) += 1; }
        for d in stats.docs.iter()  { *doc_id_freq.entry(d.clone()).or_insert(0) += 1; }
        queries.extend(stats.queries);
    }

    let total_kinds: u64 = event_kinds.values().sum();
    let event_kinds_norm: HashMap<String, f64> = event_kinds
        .into_iter()
        .map(|(k,v)| (k, v as f64 / total_kinds.max(1) as f64))
        .collect();

    let mut docs_vec: Vec<(String,u64)> = doc_id_freq.into_iter().collect();
    docs_vec.sort_by(|a,b| b.1.cmp(&a.1));
    docs_vec.truncate(5000);

    queries.truncate(200);

    Ok(Distributions {
        sample_size:         sample_n,
        events_per_session,
        event_kinds:         event_kinds_norm,
        inter_event_seconds,
        doc_ids:             docs_vec,
        queries,
    })
}
