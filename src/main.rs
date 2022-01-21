use std::{fs::File, sync::Arc, path::{Path}};

use arrow::util::pretty::pretty_format_batches;
use parquet::{file::serialized_reader::SerializedFileReader, arrow::{ParquetFileArrowReader, ArrowReader}};

use clap::Parser;


/// Dumps contents of parquet files to stdout using Rust implemetnation of arrow array reader
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Name of the file to dump
    filename: String,
}


fn main()  {
    let args = Args::parse();
    if let Err(e) = test_parquet(&args.filename) {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    } else {
        std::process::exit(0);
    }
}

/// Opens a parquet file, prints it to standardout
fn test_parquet(filename: impl AsRef<Path>) -> parquet::errors::Result<()> {
    let filename = filename.as_ref();
    println!("Testing {:?}", filename);

    let file = File::open(filename).unwrap();
    let file_reader = SerializedFileReader::new(file)?;
    let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(file_reader));

    let record_batch_reader = arrow_reader.get_record_reader(2048)?;


    let mut batches = vec![];
    for maybe_record_batch in record_batch_reader {
        // avoid buffering the entire thing by dumping every 200K rows
        if batches.len() > 100 {
            println!("{}", pretty_format_batches(&batches)?);
            batches.clear();
        }

        batches.push(maybe_record_batch?);
    }

    Ok(())
}
