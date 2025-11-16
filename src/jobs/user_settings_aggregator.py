# imports
import argparse
from ast import arg
import logging 
import sys
from pathlib import Path
from typing import Optional

from pyspark.sql import DataFrame, SparkSession

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
#sys.path.insert(0, str(Path(__file__).parent.parent))

from src.transformations.settings_transformer import UserSettingsTransformer

# Configure logger
logging.basicConfig(
    level = logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers = [logging.StreamHandler(sys.stdout)],
)

logger = logging.getLogger(__name__)

class UserSettings:

    # Initializng Spark session.
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.transformer = UserSettingsTransformer()
    
    def read_input_data(self, input_path: str, file_format: str ="csv") -> DataFrame:
        """
        Reads input data from the specified path.

        Args:
            input_path: Path to the input data.
            file_format: Format of input file.

        Returns:
            DataFrame containing user events.

        Raises:
            Exception: If the data cannot be read.
        """
        logger.info(f"Reading input data from {input_path} (format :{file_format}).")

        try:
            if file_format == "csv":
                df = self.spark.read.csv(input_path, schema = self.transformer.INPUT_SCHEMA, header = True)
            else:
                raise ValueError(f"Unsupported file format: {file_format}.")

            record_count = df.count()
            logger.info(f"Record count of the file read is: {record_count}.")
            logger.info("Successfully read the file!")

            return df
        
        except Exception as e:
            logger.error(f"Failed to read input data: {str(e)}.")
            raise
    
    def write_output_data(self, df: DataFrame, output_path: str, partition_column: str, file_format: str = "csv", mode: str = "overwrite",) -> None:
        """
        Write output data to the specified path.

        Args:
            df: DataFrame to write.
            output_path: Destination path to write the output data.
            partition_column: Column to parititon by.
            file_format: Output file format.
            mode: Write mode (Overwrite, append).

        Raises:
            Exception: If the data cannot be written.
        """
        logger.info(f"Writing output data to {output_path}(format: {file_format}).")

        try:
            writer = df.write.mode(mode)

            # Paritioning the data
            if partition_column and file_format not in ("json", "csv"):
                logger.info(f"Parititoning output by the column: {partition_column}.")
                writer = writer.partitionBy(partition_column)
            elif file_format == "csv" or file_format == "json":
                logger.info(f"Skipping paritioning.")
            else:
                raise ValueError(f"Parititon column specified is incorrect: {partition_column}.")
            
            # Write in specifed format
            if file_format == "csv":
                from pyspark.sql import functions as F
                df_csv = df.withColumn(
                    "settings", F.to_json(F.col("settings"))
                )
                writer = df_csv.write.mode(mode)
                writer.option("header", "true").csv(output_path)
            elif file_format == "json":
                df
                writer.json(output_path)
            elif file_format == "parquet":
                writer.parquet(output_path)
            else:
                raise ValueError(f"Unsupported file format: {file_format}.")
            
            logger.info(f"Successfully wrote output data to {output_path}(format: {file_format}).")
        
        except Exception as e:
            logger.error(f"Failed to write output data: {str(e)}.")
            raise

    
    def run(
        self, 
        input_path: str,
        output_path: str, 
        partition_column: str,
        input_format: str = "csv",
        output_format: str = "csv",
        )-> None:

        """
        Args:
            input_path: Path to input data.
            output_path: Path to write output data.
            partition_column: Column to parition by.
            input_format: Format of input file.
            output_format: Format of output file.
        """

        logger.info("=" * 10)
        logger.info(f"Input file path: {input_path}.")
        logger.info(f"Input file format: {input_format}.")
        logger.info(f"Partitioning column: {partition_column}.")
        logger.info(f"Output file path: {output_path}.")
        logger.info(f"Output file format: {output_format}.")
        logger.info("=" * 20)

        try:

            logger.info(f"Reading input data from: {input_path}(format: {input_format}).")
            input_df = self.read_input_data(input_path, input_format) 

            logger.info("Applying transformations.")
            output_df = self.transformer.transform(input_df)
            logger.info("Data transformations completed.")

            logger.info(f"Writing data to {output_path}(format: {output_format}).")
            self.write_output_data(output_df, output_path, partition_column, file_format=output_format)
            logger.info(f"Data written to {output_path}(format: {output_format}).")

            logger.info("=" * 20)
            logger.info("Job completed successfully!")
            logger.info("=" * 20)

        except Exception as e:
            logger.info("!" * 20 )
            logger.info(f"Job failed with the exception: {str(e)}.")
            logger.info("!" * 20)
            raise    


def parse_arguments():
    """
    Parse the passed arguments.

    Returns:
        Parsed arguments.
    """

    parser=argparse.ArgumentParser()

    parser.add_argument(
        "--input-path",
        required=True,
    )

    parser.add_argument(
        "--partition-column",
        required=True
    )

    parser.add_argument(
        "--output-path",
        required=True
    )

    parser.add_argument(
        "--input-format",
        default="csv",
        required=False
    )

    parser.add_argument(
        "--output-format",
        default="csv",
        required=False
    )

    return parser.parse_args()

def main() -> int:
    """
    Returns:
        Exit code
    """

    args = parse_arguments()

    spark = None
    exit_code = 0

    spark = SparkSession.builder.master("local[*]").appName("UserEventSpark").getOrCreate()
    
    try:
        job = UserSettings(spark)
        job.run(
            input_path=args.input_path,
            output_path=args.output_path,
            partition_column=args.partition_column,
            input_format=args.input_format,
            output_format=args.output_format,
        )
    
    except Exception as e:
        logger.info(f"Job failed with an exception: {str(e)}.")
        exit_code = 1
    
    finally:
        if spark:
            spark.stop()

    return exit_code

if __name__ == "__main__":
    sys.exit(main())



