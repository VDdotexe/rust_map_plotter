use arrow::array::Float64Array;
use arrow::csv::WriterBuilder;
use parquet::arrow::{ArrowReader, ParquetFileArrowReader};
use parquet::file::reader::SerializedFileReader;
use std::error::Error;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

fn main() -> Result<(), Box<dyn Error>> {
    let input_file = "input.parquet";
    let output_file = "output.csv";

    // Read the Parquet file
    let file = File::open(&Path::new(input_file))?;
    let reader = SerializedFileReader::new(file)?;
    let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(reader));
    let record_batch_reader = arrow_reader.get_record_reader(2048)?;

    // Create a CSV writer
    let mut writer = WriterBuilder::new().has_headers(true).build(File::create(output_file)?);

    // Process each record batch
    for maybe_batch in record_batch_reader {
        let batch = maybe_batch?;
        let schema = batch.schema();

        // Write the schema as CSV headers
        let headers: Vec<String> = schema.fields().iter().map(|f| f.name().to_string()).collect();
        writer.write_record(&headers)?;

        // Write each row to the CSV file
        for row in 0..batch.num_rows() {
            let mut record = Vec::new();
            for column in 0..batch.num_columns() {
                let array = batch.column(column);
                let value = array.as_any().downcast_ref::<Float64Array>().unwrap().value(row);
                record.push(value.to_string());
            }
            writer.write_record(&record)?;
        }
    }

    Ok(())
}