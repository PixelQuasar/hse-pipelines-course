use anyhow::Result;
use aws_sdk_s3::Client as S3Client;
use chrono::{DateTime, Datelike, Duration, Timelike, Utc};
use rand::seq::SliceRandom;
use rand::{thread_rng, Rng};
use rand_distr::{Distribution as RandDistribution, WeightedIndex};
use uuid::Uuid;

use crate::{dist::Distributions, AppConfig};

pub struct SessionStats {
    pub event_count: u32,
    pub gaps:        Vec<u32>,
    pub kinds:       Vec<String>,
    pub docs:        Vec<String>,
    pub queries:     Vec<String>,
}

pub fn scan_session(text: &str) -> SessionStats {
    let mut stats = SessionStats {
        event_count: 0, gaps: vec![], kinds: vec![], docs: vec![], queries: vec![],
    };
    let mut last_ts: Option<DateTime<Utc>> = None;
    for raw in text.lines() {
        let line = raw.trim();
        if line.is_empty() { continue; }
        let ts_opt = extract_ts(line);
        if let (Some(t), Some(prev)) = (ts_opt, last_ts) {
            let gap = (t - prev).num_seconds().max(0) as u32;
            if gap < 3600 { stats.gaps.push(gap); } // ignore obvious outliers
        }
        if let Some(t) = ts_opt { last_ts = Some(t); }

        if line.starts_with("QS ") {
            stats.kinds.push("QS".into());
            stats.event_count += 1;
            if let Some(q) = extract_query(line) { stats.queries.push(q); }
        } else if line.starts_with("CARD_SEARCH_START") {
            stats.kinds.push("CARD".into());
            stats.event_count += 1;
        } else if line.starts_with("DOC_OPEN") {
            stats.kinds.push("DOC_OPEN".into());
            stats.event_count += 1;
            if let Some(doc) = line.split_whitespace().last() {
                stats.docs.push(doc.to_string());
            }
        } else if line.chars().next().map(|c| c.is_ascii_digit() || c == '-').unwrap_or(false) {
            // results line: "<sid> doc1 doc2 ..."
            for token in line.split_whitespace().skip(1) {
                if token.contains('_') { stats.docs.push(token.to_string()); }
            }
        }
    }
    stats
}

fn extract_ts(line: &str) -> Option<DateTime<Utc>> {
    let tokens: Vec<&str> = line.split_whitespace().collect();
    if tokens.len() < 2 { return None; }
    chrono::NaiveDateTime::parse_from_str(tokens[1], "%d.%m.%Y_%H:%M:%S")
        .ok()
        .map(|nd| DateTime::<Utc>::from_naive_utc_and_offset(nd, Utc))
}

fn extract_query(line: &str) -> Option<String> {
    let start = line.find('{')?;
    let end = line.rfind('}')?;
    if end > start { Some(line[start + 1..end].to_string()) } else { None }
}

pub async fn generate_and_upload(s3: &S3Client, cfg: &AppConfig, dist: &Distributions) -> Result<String> {
    let body_utf8 = render_session(dist);
    let (encoded, _, _) = encoding_rs::WINDOWS_1251.encode(&body_utf8);

    let key = format!("synthetic-{}.txt", Uuid::new_v4());
    s3.put_object()
        .bucket(&cfg.s3_bucket)
        .key(&key)
        .body(aws_sdk_s3::primitives::ByteStream::from(encoded.into_owned()))
        .send()
        .await?;
    Ok(key)
}

fn render_session(d: &Distributions) -> String {
    let mut rng = thread_rng();

    let n_events: u32 = *d.events_per_session.choose(&mut rng).unwrap_or(&5);
    let n_events = n_events.max(1).min(50);

    let kinds: Vec<&String> = d.event_kinds.keys().collect();
    let weights: Vec<f64> = kinds.iter().map(|k| *d.event_kinds.get(*k).unwrap()).collect();
    let kind_dist = WeightedIndex::new(&weights).expect("event_kinds is empty");

    let backdate_secs = rng.gen_range(0..7 * 24 * 3600);
    let mut t = Utc::now() - Duration::seconds(backdate_secs);
    let mut out = String::new();
    out.push_str(&format!("SESSION_START {}\n", fmt_ts(t)));

    let mut last_search_id: Option<String> = None;

    for _ in 0..n_events {
        let gap = *d.inter_event_seconds.choose(&mut rng).unwrap_or(&5);
        t = t + Duration::seconds(gap as i64);

        let kind = kinds[kind_dist.sample(&mut rng)];
        match kind.as_str() {
            "QS" => {
                let q = d.queries.choose(&mut rng).cloned().unwrap_or_else(|| "test".into());
                let sid = format!("{}", rng.gen_range(1_000_000u64..999_999_999u64));
                last_search_id = Some(sid.clone());
                out.push_str(&format!("QS {} {{{}}}\n", fmt_ts(t), q));
                let n = rng.gen_range(1..20);
                let docs = sample_docs(d, &mut rng, n);
                out.push_str(&format!("{} {}\n", sid, docs.join(" ")));
            }
            "CARD" => {
                let sid = format!("{}", rng.gen_range(1_000_000u64..999_999_999u64));
                last_search_id = Some(sid.clone());
                out.push_str(&format!("CARD_SEARCH_START {}\n", fmt_ts(t)));
                let n_params = rng.gen_range(1..=2);
                for _ in 0..n_params {
                    let pid = rng.gen_range(0..200);
                    let pval = d.doc_ids.choose(&mut rng).map(|x| x.0.clone()).unwrap_or_default();
                    out.push_str(&format!("${} {}\n", pid, pval));
                }
                out.push_str("CARD_SEARCH_END\n");
                let n = rng.gen_range(1..30);
                let docs = sample_docs(d, &mut rng, n);
                out.push_str(&format!("{} {}\n", sid, docs.join(" ")));
            }
            "DOC_OPEN" => {
                if let Some(sid) = &last_search_id {
                    let doc = sample_one_doc(d, &mut rng);
                    out.push_str(&format!("DOC_OPEN {} {} {}\n", fmt_ts(t), sid, doc));
                }
            }
            _ => {}
        }
    }

    t = t + Duration::seconds(rng.gen_range(1..=15));
    out.push_str(&format!("SESSION_END {}\n", fmt_ts(t)));
    out
}

fn sample_docs<R: Rng>(d: &Distributions, rng: &mut R, n: usize) -> Vec<String> {
    (0..n).map(|_| sample_one_doc(d, rng)).collect()
}

fn sample_one_doc<R: Rng>(d: &Distributions, rng: &mut R) -> String {
    d.doc_ids.choose(rng).map(|x| x.0.clone()).unwrap_or_else(|| "LAW_0".into())
}

fn fmt_ts(t: DateTime<Utc>) -> String {
    format!(
        "{:02}.{:02}.{:04}_{:02}:{:02}:{:02}",
        t.day(), t.month(), t.year(), t.hour(), t.minute(), t.second()
    )
}
