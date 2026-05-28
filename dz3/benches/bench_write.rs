use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use polars::io::avro::AvroWriter;
use polars::prelude::*;
use std::fs::File;

fn get_loaded_dataframe() -> DataFrame {
    let parquet_file = "bench_data.parquet";
    LazyFrame::scan_parquet(PlRefPath::new(parquet_file), ScanArgsParquet::default())
        .unwrap()
        // Возьмем только 10 000 строк, чтобы тест не шел вечность,
        // но этого достаточно, чтобы увидеть скорость записи.
        .limit(10_000)
        .collect()
        .unwrap()
}

fn benchmark_write(c: &mut Criterion) {
    // 1. Сначала загружаем данные в RAM (это не входит в замер времени)
    let df: DataFrame = get_loaded_dataframe();

    let mut group = c.benchmark_group("Write (Serialization) Speed");
    group.sample_size(50); // Операция записи тяжелая, уменьшаем кол-во сэмплов

    // --- CSV WRITE ---
    // Ожидание: ПОБЕДА (или очень близко к Parquet).
    // CSV пишет просто текст, почти не нагружая CPU.
    group.bench_function("CSV Write", |b| {
        b.iter(|| {
            let mut file = File::create("temp_output.csv").unwrap();
            CsvWriter::new(&mut file).finish(&mut df.clone()).unwrap();
        })
    });

    // --- JSONL WRITE ---
    // Ожидание: Медленнее CSV (из-за дублирования ключей), но быстрее Parquet на очень маленьких батчах.
    group.bench_function("JSONL Write", |b| {
        b.iter(|| {
            let mut file = File::create("temp_output.jsonl").unwrap();
            JsonWriter::new(&mut file)
                .with_json_format(JsonFormat::JsonLines)
                .finish(&mut df.clone())
                .unwrap();
        })
    });

    // --- PARQUET WRITE ---
    // Ожидание: ПРОИГРЫШ (по процессорному времени).
    // Parquet должен:
    // 1. Проанализировать статистику каждой колонки.
    // 2. Закодировать словарем.
    // 3. Сжать через Snappy/Zstd.
    // Это оверхед.
    group.bench_function("Parquet Write (Snappy)", |b| {
        b.iter(|| {
            let mut file = File::create("temp_output.parquet").unwrap();
            ParquetWriter::new(&mut file)
                // Сжатие включено по умолчанию (Snappy)
                .finish(&mut df.clone())
                .unwrap();
        })
    });

    // --- AVRO WRITE ---
    // group.bench_function("Avro Write", |b| {
    //     b.iter(|| {
    //         let file = File::create("temp_output.avro").unwrap();
    //         AvroWriter::new(file).finish(&mut df.clone()).unwrap();
    //     })
    // });

    group.finish();

    // Удаляем временные файлы
    let _ = std::fs::remove_file("temp_output.csv");
    let _ = std::fs::remove_file("temp_output.jsonl");
    let _ = std::fs::remove_file("temp_output.parquet");
    let _ = std::fs::remove_file("temp_output.avro");
}

criterion_group!(benches, benchmark_write);
criterion_main!(benches);
