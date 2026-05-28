use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use polars::prelude::*;
use serde::Serialize;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};

// Структура одной строки датасета (упрощенная)
#[derive(Serialize, Clone)]
struct Record {
    prompt_id: String,
    // Имитация длинного текста (урезана для теста, но достаточно длинная)
    prompt: String,
    response: String,
    category: String,
    token_len: i64,
}

// Генерация фейковой записи
fn get_record() -> Record {
    Record {
        prompt_id: "id_99999".to_string(),
        prompt: "A".repeat(1000), // 1 КБ текста
        response: "B".repeat(1000),
        category: "Benchmark".to_string(),
        token_len: 42,
    }
}

// Подготовка начальных файлов (чтобы было к чему добавлять)
fn setup_files() {
    // Создаем DataFrame с 1000 строк, чтобы фаил Parquet не был пустым
    let mut df = df!(
        "Prompt_ID" => (0..1000).map(|i| i.to_string()).collect::<Vec<_>>(),
        "Prompt" => (0..1000).map(|_| "init".to_string()).collect::<Vec<_>>(),
        "Response" => (0..1000).map(|_| "init".to_string()).collect::<Vec<_>>(),
        "Category" => (0..1000).map(|_| "init".to_string()).collect::<Vec<_>>(),
        "Prompt_token_length" => (0..1000).map(|_| 10).collect::<Vec<_>>()
    )
    .unwrap();

    // 1. Создаем Parquet
    let file = File::create("append_test.parquet").unwrap();
    ParquetWriter::new(file).finish(&mut df).unwrap();

    // 2. Создаем CSV и JSONL пустыми (или с заголовком)
    let _ = File::create("append_test.csv").unwrap();
    let _ = File::create("append_test.jsonl").unwrap();

    // 3. Создаем Avro
    let file = File::create("append_test.avro").unwrap();
    polars::io::avro::AvroWriter::new(file)
        .finish(&mut df)
        .unwrap();
}

fn benchmark_append(c: &mut Criterion) {
    // Создаем начальные файлы один раз
    setup_files();
    let new_row = get_record();

    let mut group = c.benchmark_group("Append Operation (1 Row)");

    // ----------------------------------------------------------------
    // 1. JSONL APPEND (Победитель)
    // ----------------------------------------------------------------
    group.sample_size(1000); // Это быстро, можно много сэмплов
    group.bench_function("JSONL Append", |b| {
        b.iter(|| {
            // Открываем на дозапись (append=true)
            let file = OpenOptions::new()
                .create(true)
                .append(true)
                .open("append_test.jsonl")
                .unwrap();
            let mut writer = BufWriter::new(file);

            // Пишем JSON + новую строку
            serde_json::to_writer(&mut writer, &new_row).unwrap();
            writer.write_all(b"\n").unwrap();
        })
    });

    // ----------------------------------------------------------------
    // 2. CSV APPEND (Тоже очень быстро)
    // ----------------------------------------------------------------
    group.bench_function("CSV Append", |b| {
        b.iter(|| {
            let file = OpenOptions::new()
                .create(true)
                .append(true)
                .open("append_test.csv")
                .unwrap();

            let mut writer = csv::WriterBuilder::new()
                .has_headers(false) // Важно: не писать заголовок каждый раз
                .from_writer(file);

            writer.serialize(&new_row).unwrap();
            writer.flush().unwrap();
        })
    });

    // ----------------------------------------------------------------
    // 3. PARQUET "APPEND" (Катастрофически медленно)
    // ----------------------------------------------------------------
    group.sample_size(10); // Очень медленно, ставим мин. кол-во прогонов
    group.bench_function("Parquet Append (Rewrite)", |b| {
        b.iter(|| {
            // ШАГ 1: Читаем старый файл
            let mut df_old =
                LazyFrame::scan_parquet(PlRefPath::new("append_test.parquet"), Default::default())
                    .unwrap()
                    .collect()
                    .unwrap();

            // ШАГ 2: Создаем DataFrame из 1 строки
            let df_new = df!(
                "Prompt_ID" => &[new_row.prompt_id.as_str()],
                "Prompt" => &[new_row.prompt.as_str()],
                "Response" => &[new_row.response.as_str()],
                "Category" => &[new_row.category.as_str()],
                "Prompt_token_length" => &[new_row.token_len as i32]
            )
            .unwrap();

            // ШАГ 3: Склеиваем
            df_old.vstack_mut(&df_new).unwrap();

            // ШАГ 4: Полная перезапись файла
            let file = File::create("append_test.parquet").unwrap();
            ParquetWriter::new(file).finish(&mut df_old).unwrap();
        })
    });

    // ----------------------------------------------------------------
    // 4. AVRO "APPEND" (Тоже медленно, перезапись)
    // ----------------------------------------------------------------
    group.bench_function("Avro Append (Rewrite)", |b| {
        b.iter(|| {
            // ШАГ 1: Читаем старый файл (eagerly)
            let file = File::open("append_test.avro").unwrap();
            let mut df_old = polars::io::avro::AvroReader::new(file).finish().unwrap();

            // ШАГ 2: Создаем DataFrame из 1 строки
            let df_new = df!(
                "Prompt_ID" => &[new_row.prompt_id.as_str()],
                "Prompt" => &[new_row.prompt.as_str()],
                "Response" => &[new_row.response.as_str()],
                "Category" => &[new_row.category.as_str()],
                "Prompt_token_length" => &[new_row.token_len as i32]
            )
            .unwrap();

            // ШАГ 3: Склеиваем
            df_old.vstack_mut(&df_new).unwrap();

            // ШАГ 4: Полная перезапись файла
            let file = File::create("append_test.avro").unwrap();
            polars::io::avro::AvroWriter::new(file)
                .finish(&mut df_old)
                .unwrap();
        })
    });

    group.finish();

    // Удаляем мусор
    let _ = std::fs::remove_file("append_test.parquet");
    let _ = std::fs::remove_file("append_test.csv");
    let _ = std::fs::remove_file("append_test.jsonl");
    // let _ = std::fs::remove_file("append_test.avro");
}

criterion_group!(benches, benchmark_append);
criterion_main!(benches);
