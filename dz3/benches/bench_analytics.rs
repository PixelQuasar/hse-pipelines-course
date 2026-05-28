use criterion::{Criterion, criterion_group, criterion_main};
use polars::prelude::*;

fn bench_func(lf: LazyFrame) {
    lf.filter(col("Selected_score").eq(lit("first-class")))
        .group_by([col("Category"), col("Prompt_model")])
        .agg([
            col("Prompt_token_length").mean().alias("avg_input_tokens"),
            col("Response_token_length")
                .max()
                .alias("max_output_tokens"),
            len().alias("count"),
        ])
        .collect()
        .unwrap();
}

fn run_benchmark(c: &mut Criterion) {
    let parquet_file = "bench_data.parquet";
    let csv_file = "bench_data.csv";
    let json_file = "bench_data.jsonl";
    let avro_file = "bench_data.avro";

    let mut group = c.benchmark_group("Analytical Query (Group By + Agg)");

    // PARQUET
    // Ожидание: Очень быстро (100-500 итераций в секунду).
    // Читает только заголовки и нужные колонки (Column Projection).
    group.sample_size(10);
    group.bench_function("Parquet", |b| {
        b.iter(|| {
            bench_func(
                LazyFrame::scan_parquet(PlRefPath::new(parquet_file), ScanArgsParquet::default())
                    .unwrap(),
            );
        })
    });

    // CSV
    // Ожидание: Очень медленно.
    // Вынужден парсить гигабайты текста columns 'Prompt' и 'Response',
    // хотя они нам даже не нужны в запросе.
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
    // Ожидание: Самый медленный.
    // Парсинг структуры JSON для каждой строки + чтение ненужного текста.
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

    // group.sample_size(10);
    // group.bench_function("Avro", |b| {
    //     b.iter(|| {
    //         let file = std::fs::File::open(avro_file).unwrap();
    //         let df = polars::io::avro::AvroReader::new(file).finish().unwrap();
    //         bench_func(df.lazy())
    //     })
    // });

    group.finish();
}

criterion_group!(benches, run_benchmark);
criterion_main!(benches);
