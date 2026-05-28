use criterion::{Criterion, criterion_group, criterion_main};
use polars::prelude::*;

fn bench_func(lf: LazyFrame) {
    lf.select([col("Prompt").str().len_chars(), col("Category")])
        .filter(col("Category").eq(lit("Music")))
        .select([col("Prompt").sum()])
        .collect()
        .unwrap();
}

fn run_benchmark(c: &mut Criterion) {
    let parquet_file = "bench_data.parquet";
    let csv_file = "bench_data.csv";
    let json_file = "bench_data.jsonl";
    let avro_file = "bench_data.avro";

    let mut group = c.benchmark_group("File IO Performance");

    group.sample_size(10);
    group.bench_function("Avro", |b| {
        b.iter(|| {
            let file = std::fs::File::open(avro_file).unwrap();
            let df = polars::io::avro::AvroReader::new(file).finish().unwrap();
            bench_func(df.lazy())
        })
    });

    group.sample_size(10);
    group.bench_function("Parquet", |b| {
        b.iter(|| {
            bench_func(
                LazyFrame::scan_parquet(PlRefPath::new(parquet_file), ScanArgsParquet::default())
                    .unwrap(),
            );
        })
    });

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
