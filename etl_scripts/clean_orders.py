from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType
import logging
import sys

# Configure logging for better visibility and debugging during job execution.
logging.basicConfig(
    level=logging.INFO, # Set the logging level to INFO to capture general progress and important messages.
    format="%(asctime)s [%(levelname)s] %(message)s" # Define the format for log messages (timestamp, level, message).
)
logger = logging.getLogger() # Get the root logger instance to use throughout the script.

try:
    logger.info("Starting Spark session with Delta Lake support...")
    # Initialize SparkSession, which is the entry point to Spark functionality.
    # .appName(): Sets a name for the Spark application, useful for monitoring.
    # .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"): Enables Delta Lake SQL syntax and features.
    # .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"): Configures Spark to use Delta Lake's catalog for table management.
    # .getOrCreate(): Gets an existing SparkSession or creates a new one if none exists.
    spark = SparkSession.builder \
        .appName("CleanOrdersETL") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    logger.info("Defining schema for orders.csv")
    # Define the explicit schema for the 'orders' dataset.
    # This is crucial for schema enforcement, ensuring data types are correctly interpreted from the CSV
    # and preventing data quality issues. 'True' indicates that fields are nullable.
    schema = StructType([
        StructField("order_num", IntegerType(), True), # Original order number, potentially from a legacy system.
        StructField("order_id", IntegerType(), True), # Unique identifier for each order.
        StructField("user_id", IntegerType(), True), # Identifier for the user who placed the order.
        StructField("order_timestamp", StringType(), True), # The timestamp of the order, initially as a string.
        StructField("total_amount", FloatType(), True), # The total monetary amount of the order.
        StructField("date", StringType(), True) # The date of the order, used for partitioning.
    ])

    # Define the S3 path where the raw 'orders.csv' data is located.
    raw_path = "s3://lab5lakehouse/lakehouse/raw/orders/orders.csv"
    logger.info(f"Reading raw data from {raw_path}")
    # Read the raw CSV data into a Spark DataFrame.
    # .option("header", "true"): Indicates that the first row of the CSV contains column headers.
    # .schema(schema): Applies the predefined schema to the DataFrame, enforcing data types.
    df = spark.read.format("csv").option("header", "true").schema(schema).load(raw_path)

    logger.info("Filtering rows with null order_id or user_id")
    # Filter the DataFrame to keep only valid records.
    # Records are considered valid if 'order_id' and 'user_id' (critical primary/foreign keys) are not null.
    valid_df = df.filter(col("order_id").isNotNull() & col("user_id").isNotNull())

    logger.info("Parsing order_timestamp column to timestamp format")
    # Convert the 'order_timestamp' column from its initial StringType to a proper TimestampType.
    # This enables time-based operations and consistency with other timestamp fields.
    valid_df = valid_df.withColumn("order_timestamp", to_timestamp("order_timestamp"))

    logger.info("Removing duplicates using order_id and order_timestamp")
    # Deduplicate records based on a combination of 'order_id' and 'order_timestamp'.
    # This ensures that each unique order instance (identified by its ID and exact time) is present only once.
    deduped_df = valid_df.dropDuplicates(["order_id", "order_timestamp"])

    # Define the S3 path where the cleaned and processed Delta Lake table for orders will be stored.
    clean_output_path = "s3://lab5lakehouse/lakehouse-dwh/orders/"
    logger.info(f"Writing clean data to Delta Lake at {clean_output_path}")
    # Write the deduplicated and cleaned DataFrame to the specified S3 path in Delta Lake format.
    # .format("delta"): Specifies the output format as Delta Lake.
    # .mode("overwrite"): Overwrites the entire Delta table if it already exists (suitable for initial loads or full refreshes).
    # .partitionBy("date"): Partitions the data by the 'date' column. This optimizes query performance
    # by allowing query engines (like Athena) to scan only relevant data partitions.
    deduped_df.write.format("delta").mode("overwrite").partitionBy("date").save(clean_output_path)

    logger.info("Filtering invalid records for rejection")
    # Identify records that were filtered out during the validation step (i.e., those with null 'order_id' or 'user_id').
    invalid_df = df.filter(col("order_id").isNull() | col("user_id").isNull())
    # Define the S3 path for storing rejected (invalid) records.
    rejected_path = "s3://lab5lakehouse/lakehouse/rejected/orders/"
    invalid_count = invalid_df.count() # Count the number of invalid records found.

    # Check if there are any invalid records.
    if invalid_count > 0:
        logger.warning(f"Found {invalid_count} invalid rows. Writing to {rejected_path}")
        # Write the invalid records to the rejected zone in CSV format.
        # 'mode("overwrite")': Overwrites any previously rejected files in this path.
        # 'header=True': Includes column headers in the output CSV file.
        invalid_df.write.mode("overwrite").csv(rejected_path, header=True)
    else:
        logger.info("No invalid rows found.") # Log if no invalid records were found.

    logger.info("ETL job for orders completed successfully.")

except Exception as e:
    # Catch any exceptions that occur during the ETL process.
    # Log the error message and include traceback information for detailed debugging.
    logger.error(f"ETL job failed due to error: {str(e)}", exc_info=True)
    sys.exit(1) # Exit the script with a non-zero status code to indicate failure to the orchestrator (e.g., Step Functions).

finally:
    # This block ensures that the SparkSession is always stopped,
    # regardless of whether the job succeeded or failed, to release resources.
    spark.stop()
    logger.info("Spark session stopped.")
