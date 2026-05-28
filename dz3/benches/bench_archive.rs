use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use polars::io::avro::AvroWriter;
use polars::prelude::*;
use std::fs::{self, File};

// Настройки
const SAMPLE_SIZE: usize = 10; // Мало сэмплов, так как запись - операция медленная
const ROWS_LIMIT: usize = 10_000; // Ограничиваем датасет для теста скорости

fn load_data() -> DataFrame {
    let parquet_file = "bench_data.parquet";
    LazyFrame::scan_parquet(PlRefPath::new(parquet_file), ScanArgsParquet::default())
        .unwrap()
        .limit(ROWS_LIMIT as u32)
        .collect()
        .unwrap()
}

// Вспомогательная функция для генерации файлов и получения их размера
// (Запускается один раз перед бенчмарком для статистики)
fn calculate_sizes(df: &mut DataFrame) {
    println!("\n{:=^60}", " COMPRESSION REPORT (Size vs Algo) ");
    println!(
        "{:<20} | {:<15} | {:<10}",
        "Format", "Size", "Ratio (vs CSV)"
    );
    println!("{:-<20}-|-{:-<15}-|-{:-<10}", "", "", "");

    // 1. CSV
    let _ = File::create("size_test.csv").unwrap();
    let mut f = File::create("size_test.csv").unwrap();
    CsvWriter::new(&mut f).finish(df).unwrap();
    let size_csv = fs::metadata("size_test.csv").unwrap().len();

    // 2. JSONL
    let _ = File::create("size_test.jsonl").unwrap();
    let mut f = File::create("size_test.jsonl").unwrap();
    JsonWriter::new(&mut f)
        .with_json_format(JsonFormat::JsonLines)
        .finish(df)
        .unwrap();
    let size_json = fs::metadata("size_test.jsonl").unwrap().len();

    // 3. Parquet Snappy
    let _ = File::create("size_test_snappy.parquet").unwrap();
    let mut f = File::create("size_test_snappy.parquet").unwrap();
    ParquetWriter::new(&mut f)
        .with_compression(ParquetCompression::Snappy)
        .finish(df)
        .unwrap();
    let size_snappy = fs::metadata("size_test_snappy.parquet").unwrap().len();

    // 4. Parquet Zstd
    let _ = File::create("size_test_zstd.parquet").unwrap();
    let mut f = File::create("size_test_zstd.parquet").unwrap();
    ParquetWriter::new(&mut f)
        .with_compression(ParquetCompression::Zstd(None))
        .finish(df)
        .unwrap();
    let size_zstd = fs::metadata("size_test_zstd.parquet").unwrap().len();

    // 5. Avro
    let _ = File::create("size_test.avro").unwrap();
    let f = File::create("size_test.avro").unwrap();
    AvroWriter::new(f).finish(df).unwrap();
    let size_avro = fs::metadata("size_test.avro").unwrap().len();

    // Вывод таблицы
    print_row("CSV (Raw)", size_csv, size_csv);
    print_row("JSONL", size_json, size_csv);
    print_row("Parquet (Snappy)", size_snappy, size_csv);
    print_row("Parquet (Zstd)", size_zstd, size_csv);
    print_row("Avro", size_avro, size_csv);

    println!("{:=^60}\n", " STARTING TIME BENCHMARKS ");

    // Чистим
    let _ = fs::remove_file("size_test.csv");
    let _ = fs::remove_file("size_test.jsonl");
    let _ = fs::remove_file("size_test_snappy.parquet");
    let _ = fs::remove_file("size_test_zstd.parquet");
    let _ = fs::remove_file("size_test.avro");
}

fn print_row(name: &str, size: u64, base: u64) {
    let mb = size as f64 / 1024.0 / 1024.0;
    let ratio = (size as f64 / base as f64) * 100.0;
    println!("{:<20} | {:>10.2} MB | {:>9.1}%", name, mb, ratio);
}

fn benchmark_compression(c: &mut Criterion) {
    // 1. Загружаем данные
    let mut df = load_data();

    // 2. Показываем размеры (просто принтом в консоль)
    calculate_sizes(&mut df);

    // 3. Замеряем ВРЕМЯ сжатия
    let mut group = c.benchmark_group("Compression Speed (Write Time)");
    group.sample_size(SAMPLE_SIZE);

    // Для расчета Throughput возьмем размер сырого CSV как базу "объема информации"
    // Так мы поймем скорость обработки исходных байтов.
    let _ = File::create("temp_base.csv").unwrap();
    let mut f = File::create("temp_base.csv").unwrap();
    CsvWriter::new(&mut f).finish(&mut df.clone()).unwrap();
    let raw_size = fs::metadata("temp_base.csv").unwrap().len();
    let _ = fs::remove_file("temp_base.csv");

    group.throughput(Throughput::Bytes(raw_size));

    // --- CSV (Baseline) ---
    group.bench_function("CSV Write", |b| {
        b.iter(|| {
            let mut file = File::create("temp.csv").unwrap();
            CsvWriter::new(&mut file).finish(&mut df.clone()).unwrap();
        })
    });

    // --- Parquet Snappy (Standard) ---
    group.bench_function("Parquet Snappy", |b| {
        b.iter(|| {
            let mut file = File::create("temp.parquet").unwrap();
            ParquetWriter::new(&mut file)
                .with_compression(ParquetCompression::Snappy)
                .finish(&mut df.clone())
                .unwrap();
        })
    });

    // --- Parquet Zstd (High Compression) ---
    // Zstd обычно медленнее Snappy, но сжимает лучше. Проверим, насколько медленнее.
    group.bench_function("Parquet Zstd", |b| {
        b.iter(|| {
            let mut file = File::create("temp.parquet").unwrap();
            ParquetWriter::new(&mut file)
                .with_compression(ParquetCompression::Zstd(None))
                .finish(&mut df.clone())
                .unwrap();
        })
    });

    // --- Avro ---
    group.bench_function("Avro", |b| {
        b.iter(|| {
            let file = File::create("temp.avro").unwrap();
            AvroWriter::new(file).finish(&mut df.clone()).unwrap();
        })
    });

    group.finish();

    let _ = fs::remove_file("temp.csv");
    let _ = fs::remove_file("temp.parquet");
    let _ = fs::remove_file("temp.avro");
}

criterion_group!(benches, benchmark_compression);
criterion_main!(benches);
