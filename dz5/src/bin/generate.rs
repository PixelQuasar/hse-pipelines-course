use polars::prelude::*;
use rand::prelude::*;
use std::fs::File;
use std::time::Instant;

const ROW_COUNT: usize = 20_000_000;

// Справочники для генерации реалистичных данных
const USER_AGENTS: &[&str] = &[
    "Mozilla/5.0 (iPhone; CPU iPhone OS 14_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148",
    "Mozilla/5.0 (Linux; Android 11; SM-G991B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.120 Mobile Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
    "Mozilla/5.0 (Linux; Android 10; SM-A505FN) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.120 Mobile Safari/537.36",
    "Mozilla/5.0 (iPad; CPU OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
];

const AD_SIZES: &[&str] = &[
    "300x250", "728x90", "160x600", "320x50", "300x600", "970x250",
];
const DOMAINS: &[&str] = &[
    "news.com",
    "sports.net",
    "weather.io",
    "finance.org",
    "tech.blog",
    "games.gg",
    "music.fm",
];

fn main() -> PolarsResult<()> {
    let start = Instant::now();
    println!("🚀 Начинаем генерацию AdTech датасета v2...");

    // 1. Генерация справочника GEO (Small Data)
    println!("🌍 Генерируем справочник Geo...");
    let geo_ids: Vec<i32> = (0..200).collect();
    let country_codes: Vec<String> = (0..200)
        .map(|i| match i {
            0 => "RU".to_string(),
            1 => "US".to_string(),
            2 => "DE".to_string(),
            3 => "FR".to_string(),
            4 => "GB".to_string(),
            _ => format!("CN_{}", i),
        })
        .collect();

    let mut df_geo = df!(
        "geo_id" => geo_ids,
        "country_code" => country_codes
    )?;

    let file = File::create("geo_dict.parquet").expect("Could not create file");
    ParquetWriter::new(file).finish(&mut df_geo)?;

    // 2. Генерация справочника Publisher Categories (для lookup бенчмарка)
    println!("📚 Генерируем справочник Publisher Categories...");
    let mut pub_ids_dict: Vec<String> =
        vec!["pub_huge_whale".to_string(), "pub_medium_fish".to_string()];
    for i in 0..1000 {
        pub_ids_dict.push(format!("pub_small_{}", i));
    }

    let categories: Vec<String> = pub_ids_dict
        .iter()
        .enumerate()
        .map(|(i, _)| match i {
            0 => "premium".to_string(),
            1 => "standard".to_string(),
            _ => {
                if i % 3 == 0 {
                    "economy".to_string()
                } else if i % 3 == 1 {
                    "standard".to_string()
                } else {
                    "unverified".to_string()
                }
            }
        })
        .collect();

    let mut df_pub_categories = df!(
        "publisher_id" => pub_ids_dict,
        "category" => categories
    )?;

    let file_pub = File::create("publisher_categories.parquet").expect("Could not create file");
    ParquetWriter::new(file_pub).finish(&mut df_pub_categories)?;

    // 3. Генерация Impressions с новыми полями
    println!("📊 Генерируем Impressions ({} строк)...", ROW_COUNT);

    let mut rng = rand::rng();

    let mut transaction_ids = Vec::with_capacity(ROW_COUNT);
    let mut publisher_ids = Vec::with_capacity(ROW_COUNT);
    let mut geo_ids_col = Vec::with_capacity(ROW_COUNT);
    let mut bid_prices = Vec::with_capacity(ROW_COUNT);
    let mut timestamps = Vec::with_capacity(ROW_COUNT);

    // Новые поля для бенчмарка сериализации
    let mut user_agents = Vec::with_capacity(ROW_COUNT);
    let mut ip_addresses = Vec::with_capacity(ROW_COUNT);
    let mut bid_request_jsons = Vec::with_capacity(ROW_COUNT);

    for i in 0..ROW_COUNT {
        let r: f64 = rng.random();

        // Publisher ID с перекосом
        let pub_id = if r < 0.70 {
            "pub_huge_whale".to_string()
        } else if r < 0.85 {
            "pub_medium_fish".to_string()
        } else {
            format!("pub_small_{}", rng.random_range(0..1000))
        };
        publisher_ids.push(pub_id);

        // Geo ID
        geo_ids_col.push(rng.random_range(0..200) as i32);

        // Bid price (5% нулевых)
        let price = if rng.random_bool(0.05) {
            0.0
        } else {
            (rng.random::<f64>() * 10.0 * 100.0).round() / 100.0
        };
        bid_prices.push(price);

        // Timestamp
        timestamps.push(1672531200 + i as i64);

        // Transaction ID
        transaction_ids.push(format!("tx_{}", i));

        // === НОВЫЕ ПОЛЯ ===

        // User Agent (для парсинга device type)
        let ua = USER_AGENTS[rng.random_range(0..USER_AGENTS.len())];
        user_agents.push(ua.to_string());

        // IP Address (для определения internal/external)
        let ip = if rng.random_bool(0.1) {
            // 10% internal IPs
            format!(
                "192.168.{}.{}",
                rng.random_range(0..256),
                rng.random_range(1..255)
            )
        } else if rng.random_bool(0.05) {
            // 5% localhost
            format!(
                "10.0.{}.{}",
                rng.random_range(0..256),
                rng.random_range(1..255)
            )
        } else {
            // External IPs
            format!(
                "{}.{}.{}.{}",
                rng.random_range(1..224),
                rng.random_range(0..256),
                rng.random_range(0..256),
                rng.random_range(1..255)
            )
        };
        ip_addresses.push(ip);

        // Bid Request JSON (для парсинга вложенных данных)
        let ad_size = AD_SIZES[rng.random_range(0..AD_SIZES.len())];
        let domain = DOMAINS[rng.random_range(0..DOMAINS.len())];
        let floor_price = (rng.random::<f64>() * 2.0 * 100.0).round() / 100.0;
        let viewability = rng.random_range(30..100);

        let json = format!(
            r#"{{"ad_size":"{}","floor_price":{},"domain":"{}","viewability":{},"gdpr_consent":{}}}"#,
            ad_size,
            floor_price,
            domain,
            viewability,
            rng.random_bool(0.8) // 80% имеют согласие
        );
        bid_request_jsons.push(json);
    }

    // Собираем DataFrame
    let mut df_impressions = df!(
        "transaction_id" => transaction_ids,
        "publisher_id" => publisher_ids,
        "geo_id" => geo_ids_col,
        "bid_price" => bid_prices,
        "timestamp" => timestamps,
        "user_agent" => user_agents,
        "ip_address" => ip_addresses,
        "bid_request" => bid_request_jsons
    )?;

    // Сохраняем
    let file_imp = File::create("impressions.parquet").expect("Could not create file");
    ParquetWriter::new(file_imp)
        .with_compression(ParquetCompression::Snappy)
        .finish(&mut df_impressions)?;

    let duration = start.elapsed();
    println!(
        "✅ Готово! Сгенерировано {} строк за {:.2?}",
        ROW_COUNT, duration
    );
    println!("📂 Файлы созданы:");
    println!("   - impressions.parquet (основные данные)");
    println!("   - geo_dict.parquet (справочник гео)");
    println!("   - publisher_categories.parquet (справочник категорий)");

    Ok(())
}
