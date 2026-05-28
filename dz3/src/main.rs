use anyhow::Result;
use polars::prelude::*;

fn main() -> Result<()> {
    let parquet_file = "bench_data.parquet";

    println!("Reading parquet file: {}", parquet_file);
    let mut df = LazyFrame::scan_parquet(PlRefPath::new(parquet_file), ScanArgsParquet::default())?
        .collect()?;

    Ok(())
}

/*

Prompt_ID
stringlengths
32

Prompt
stringlengths
143.23k

Response
stringlengths
151.7k

Prompt_model
stringclasses
llama3-8b-8192

Response_model
stringclasses
Llama3-8b

Category
stringlengths
1120

Subcategory
stringlengths
2105

Selected_score
stringclasses
first-class

Selected_length
stringclasses
very short

Selected_style
stringclasses
absurdist

Prompt_method
stringclasses
Meta

Prompt_token_length
int64
4764

Response_token_length
int64

*/
