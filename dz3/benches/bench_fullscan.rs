use criterion::{Criterion, criterion_group, criterion_main};
use polars::prelude::*;

fn bench_func(lf: LazyFrame) {
    // SCENARIO: FULL DATASET SCAN
    // Мы эмулируем ситуацию "холодного старта", когда нужны данные из самых тяжелых колонок.
    // Мы запрашиваем длину строк для Prompt и Response.
    // Для CSV/JSON это ад: нужно прочитать и разэкранировать 150к символов в каждой строке.
    // Для Parquet это тоже нагрузка: нужно декомпрессировать (Snappy) огромные блоки.
    lf.select([
        col("Prompt")
            .str()
            .len_chars()
            .sum()
            .alias("total_prompt_chars"),
        col("Response")
            .str()
            .len_chars()
            .sum()
            .alias("total_response_chars"),
        col("Category").n_unique().alias("unique_categories"),
        col("Prompt_token_length").mean().alias("avg_tokens"),
    ])
    .collect()
    .unwrap();
}

fn run_benchmark(c: &mut Criterion) {
    let parquet_file = "bench_data.parquet";
    let csv_file = "bench_data.csv";
    let json_file = "bench_data.jsonl";

    let mut group = c.benchmark_group("Full Scan (Heavy Text Load)");

    // Parquet
    // Ожидание: Быстрее всех, но процессор будет нагружен декомпрессией Snappy.
    group.sample_size(50);
    group.bench_function("Parquet", |b| {
        b.iter(|| {
            bench_func(
                LazyFrame::scan_parquet(PlRefPath::new(parquet_file), ScanArgsParquet::default())
                    .unwrap(),
            );
        })
    });

    // CSV
    // Ожидание: Медленнее.
    // Поскольку внутри Prompt есть переносы строк `\n`, CSV парсер не может просто
    // скакать по строкам, ему нужно честно парсить кавычки.
    group.sample_size(10);
    group.bench_function("CSV", |b| {
        b.iter(|| {
            bench_func(
                LazyCsvReader::new(PlRefPath::new(csv_file))
                    .finish()
                    .unwrap(),
            );
        })
    });

    // JSONL
    // Ожидание: Аутсайдер.
    // Огромный оверхед на чтение ключей "Prompt": ... и "Response": ...
    // которые повторяются в каждой строке.
    group.sample_size(10);
    group.bench_function("JSONL", |b| {
        b.iter(|| {
            bench_func(
                LazyJsonLineReader::new(PlRefPath::new(json_file))
                    .finish()
                    .unwrap(),
            )
        })
    });

    group.finish();
}

criterion_group!(benches, run_benchmark);
criterion_main!(benches);
