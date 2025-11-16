# Imports
import logging 
from typing import Any, List, Set

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

# Initializing logger
logger = logging.getLogger(__name__)

class UserSettingsTransformer:

    # Schema constraints - input
    INPUT_SCHEMA = T.StructType([
        T.StructField("id", T.LongType(), nullable = False),
        T.StructField("name", T.StringType(), nullable = False),
        T.StructField("value", T.StringType(), nullable = False),
        T.StructField("timestamp", T.LongType(), nullable = False)
    ])

    # Schema constraints - output
    OUTPUT_SCHEMA = T.StructType([
        T.StructField("id", T.LongType(), nullable = False),
        T.StructField("settings", T.MapType(T.StringType(), T.StringType()), nullable = False)
    ])

    def __init__(self):
        self.input_columns = ["id", "name", "value", "timestamp"]
    
    # Column and data type validation
    def validate_input_schema(self, df: DataFrame) -> None:

        logger.info("Input schema validation started.")

        # Validate for required columns
        missing_cols = set(self.input_columns) - set(df.columns)
        if missing_cols:
            raise ValueError(
                f"Input DataFrame is missing required columns : {missing_cols}."
            ) 

        # Validate for correct data types
        schema_dict = {field.name: field.dataType for field in df.schema.fields}
      
        expected_types = {
            "id": T.LongType(),
            "name": T.StringType(),
            "value": T.StringType(),
            "timestamp": T.LongType()
        }

        for col_name, expected_type in expected_types.items():
            actual_type = schema_dict.get(col_name)
            if not isinstance(actual_type, type(expected_type)):
                raise ValueError(
                    f"Column {col_name} has data type: {actual_type}. Expected is {expected_type}."
                )
       
        logger.info("Input schema validation passed.")
   
   # Deduplication to keep only the record with the latest timestamp for each id, name pair.
    def deduplicate_by_latest_timestamp(self, df: DataFrame) -> DataFrame:

        logger.info("Deduplication process start.") 

        # Window partitioned by id, name and ordered by timestamp in descending.
        window_spec = Window.partitionBy("id", "name").orderBy(F.desc("timestamp"))

        # Applying row number function over the window, to get the latest record.
        df_with_row_num = df.withColumn("row_num", F.row_number().over(window_spec))

        # Filtering out for row number = 1 to keep only the latest record.
        deduplicated_df = df_with_row_num.filter(F.col("row_num") == 1).drop("row_num")

        logger.info("Deduplication process completed.")
        logger.info(f"Record count before deduplication: {df.count()}.")
        logger.info(f"Record count post deduplication: {deduplicated_df.count()}.")

        return deduplicated_df
    
    # Aggregate names and values into a Map for each user.
    def aggregate_to_map(self, df: DataFrame) -> DataFrame:

        logger.info("Aggregation process start.")

        output_df = df.groupBy("id").agg(
            F.map_from_entries(
                F.collect_list(
                    F.struct(
                        F.col("name").alias("key"),
                        F.col("value").alias("value")
                    )
                )
            ).alias("settings")
        )

        logger.info("Aggregation completed.")

        return output_df

    
    def transform(self, df: DataFrame) -> DataFrame:

        logger.info("Starting user settings transformation.")

        # Step 1: Validate the DataFrame 
        self.validate_input_schema(df)

        # Step 2: Deduplicate by latest timestamp
        deduplicated_df = self.deduplicate_by_latest_timestamp(df)

        # Step 3: Aggregate names and values into a Map for each user.
        output_df = self.aggregate_to_map(deduplicated_df)

        logger.info("Transformation completed successfully.")

        return output_df

def transform_user_settings(df: DataFrame) -> DataFrame:

    transformer = UserSettingsTransformer()
    return transformer.transform(df)

