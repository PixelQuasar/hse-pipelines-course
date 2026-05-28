use anyhow::{Context, Result};
use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use aws_sdk_s3::config::Region;
use rand::{thread_rng, Rng};
use std::env;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing_subscriber::EnvFilter;

mod dist;
mod session;

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub s3_endpoint:         String,
    pub s3_bucket:           String,
    pub s3_region:           String,
    pub aws_access_key:      String,
    pub aws_secret_key:      String,
    pub sessions_per_window: usize,
    pub window_seconds:      f64,
    pub dist_cache_path:     String,
}

impl AppConfig {
    fn from_env() -> Result<Self> {
        Ok(Self {
            s3_endpoint:         env::var("S3_ENDPOINT").unwrap_or_else(|_| "http://minio:9000".into()),
            s3_bucket:           env::var("S3_BUCKET").unwrap_or_else(|_| "sessions".into()),
            s3_region:           env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".into()),
            aws_access_key:      env::var("AWS_ACCESS_KEY_ID").context("AWS_ACCESS_KEY_ID")?,
            aws_secret_key:      env::var("AWS_SECRET_ACCESS_KEY").context("AWS_SECRET_ACCESS_KEY")?,
            sessions_per_window: env::var("SESSIONS_PER_HOUR").unwrap_or_else(|_| "10000".into()).parse()?,
            window_seconds:      env::var("BATCH_WINDOW_SECONDS").unwrap_or_else(|_| "3600".into()).parse()?,
            dist_cache_path:     env::var("DIST_CACHE_PATH")
                .unwrap_or_else(|_| "/var/cache/generator/distributions.json".into()),
        })
    }
}

async fn build_s3(cfg: &AppConfig) -> Result<aws_sdk_s3::Client> {
    let creds = Credentials::new(&cfg.aws_access_key, &cfg.aws_secret_key, None, None, "static");
    let conf = aws_config::defaults(BehaviorVersion::latest())
        .region(Region::new(cfg.s3_region.clone()))
        .endpoint_url(&cfg.s3_endpoint)
        .credentials_provider(creds)
        .load()
        .await;
    let s3_conf = aws_sdk_s3::config::Builder::from(&conf)
        .force_path_style(true)
        .build();
    Ok(aws_sdk_s3::Client::from_conf(s3_conf))
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let cfg = AppConfig::from_env()?;
    tracing::info!(
        endpoint = %cfg.s3_endpoint,
        bucket = %cfg.s3_bucket,
        sessions_per_window = cfg.sessions_per_window,
        window_s = cfg.window_seconds,
        "generator starting"
    );

    let s3 = build_s3(&cfg).await?;
    let dist = Arc::new(dist::load_or_snapshot(&s3, &cfg).await?);
    tracing::info!(
        sample_size = dist.sample_size,
        kinds = ?dist.event_kinds.keys().collect::<Vec<_>>(),
        docs = dist.doc_ids.len(),
        "distributions ready"
    );
    let cfg = Arc::new(cfg);

    loop {
        let batch_start = Instant::now();
        let arrivals = plan_arrivals(cfg.sessions_per_window, cfg.window_seconds);
        tracing::info!(
            n = arrivals.len(),
            window_s = cfg.window_seconds,
            "batch scheduled — sessions will trickle in across the window"
        );

        for offset_s in arrivals {
            let target = batch_start + Duration::from_secs_f64(offset_s);
            let now = Instant::now();
            if target > now {
                tokio::time::sleep(target - now).await;
            }
            let s3   = s3.clone();
            let cfg  = cfg.clone();
            let dist = dist.clone();
            tokio::spawn(async move {
                match session::generate_and_upload(&s3, &cfg, &dist).await {
                    Ok(key) => tracing::debug!(key, "uploaded synthetic session"),
                    Err(e)  => tracing::error!(error = %e, "generation failed"),
                }
            });
        }

        let elapsed = batch_start.elapsed();
        let window  = Duration::from_secs_f64(cfg.window_seconds);
        if elapsed < window {
            tokio::time::sleep(window - elapsed).await;
        }
        tracing::info!("batch window complete; planning next window");
    }
}

fn plan_arrivals(n: usize, duration_s: f64) -> Vec<f64> {
    use std::f64::consts::PI;
    let mut rng = thread_rng();
    let f_max = 1.0 + 0.5 + 0.3;
    let mut arrivals = Vec::with_capacity(n);
    while arrivals.len() < n {
        let t = rng.gen_range(0.0..duration_s);
        let u = t / duration_s;
        let f = 1.0
              + 0.5 * (2.0 * PI * u).sin()
              + 0.3 * (2.0 * PI * 3.0 * u).sin();
        if rng.gen::<f64>() < (f / f_max) {
            arrivals.push(t);
        }
    }
    arrivals.sort_by(|a, b| a.partial_cmp(b).unwrap());
    arrivals
}
