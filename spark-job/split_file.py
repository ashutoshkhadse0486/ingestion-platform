"""Split large CSV or JSON datasets into smaller chunks for downstream processing."""

import argparse
import json
import logging
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, floor, row_number
from pyspark.sql.window import Window

LOGGER = logging.getLogger("unified-file-splitter")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")

def split_file(input_path: str, output_directory: str, chunk_size: int, file_type: str) -> list[str]:
    """Chunk a CSV or JSON source file into smaller Spark output partitions."""
    LOGGER.info("Starting file split: input=%s, output=%s, chunk_size=%s, file_type=%s", input_path, output_directory, chunk_size, file_type)
    spark = SparkSession.builder.appName("unified-file-splitter").getOrCreate()
    
    try:
        file_type_upper = file_type.upper()
        
        # Read files dynamically based on type configuration
        if file_type_upper == "CSV":
            df = spark.read.option("header", True).option("inferSchema", True).csv(input_path)
        elif file_type_upper == "JSON":
            df = spark.read.option("multiLine", True).json(input_path)
        else:
            raise ValueError(f"Unsupported format: {file_type}. Use 'CSV' or 'JSON'.")

        LOGGER.info("Loaded input dataset: input=%s, columns=%s", input_path, df.columns)
            
        # Window ranking to calculate the chunk bucket index per record
        # Uses df.columns to order dynamically without needing a hardcoded key
        window_spec = Window.orderBy(col(df.columns[0])) 
        df_chunked = df.withColumn("row_idx", row_number().over(window_spec) - 1) \
                       .withColumn("chunk_id", floor(col("row_idx") / chunk_size))
        
        # Drop processing columns and initialize the data writer
        writer = df_chunked.drop("row_idx").write.mode("overwrite").partitionBy("chunk_id")
        
        # Route to the correct format writer
        if file_type_upper == "CSV":
            writer.option("header", True).csv(output_directory)
        elif file_type_upper == "JSON":
            writer.json(output_directory)

        # Discover and sort the generated split part-files
        output_path = Path(output_directory)
        split_files = [str(path) for path in sorted(output_path.glob("**/part-*"))]
        if not split_files:
            LOGGER.warning("Split completed without output files: output=%s", output_directory)
        else:
            LOGGER.info("Split completed: output=%s, file_count=%s", output_directory, len(split_files))
        print(json.dumps(split_files))
        return split_files
    except Exception:
        LOGGER.exception("File split failed: input=%s, output=%s, file_type=%s", input_path, output_directory, file_type)
        raise
    finally:
        LOGGER.info("Stopping Spark session: input=%s", input_path)
        spark.stop()

def main() -> None:
    """CLI entry point for the splitter job."""
    parser = argparse.ArgumentParser(description="Unified CSV and JSON file chunk splitter")
    parser.add_argument("--input", required=True, help="Path to the input CSV or JSON dataset")
    parser.add_argument("--output-directory", required=True, help="Destination directory for chunks")
    parser.add_argument("--chunk-size", required=True, type=int, help="Number of records per output file")
    parser.add_argument("--file-type", required=True, choices=["JSON", "CSV"], help="Input file extension type")
    arguments = parser.parse_args()

    if arguments.chunk_size <= 0:
        LOGGER.error("Invalid chunk size: chunk_size=%s", arguments.chunk_size)
        raise ValueError("chunk-size must be greater than zero")

    try:
        split_file(arguments.input, arguments.output_directory, arguments.chunk_size, arguments.file_type)
    except Exception:
        LOGGER.exception("Splitter command failed: input=%s, output=%s", arguments.input, arguments.output_directory)
        raise

if __name__ == "__main__":
    main()