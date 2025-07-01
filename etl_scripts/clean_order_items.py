from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import logging
import sys

# Setup logging for better visibility into job execution
logging.basicConfig(
    level=logging.INFO, # Set logging level to INFO to capture general progress
    format="%(asctime)s [%(levelname)s] %(message)s" # Define log message format
)
logger = logging.getLogger() # Get the root logger instance

try:
    logger.info("Starting Spark session with Delta Lake support")
    # Initialize SparkSession with necessary configurations for Delta Lake.
    # spark.sql.extensions: Enables Delta Lake SQL commands.
    # spark.sql.catalog.spark_catalog: Configures Spark to use DeltaCatalog for table management.
    spark = SparkSession.builder \
        .appName("CleanOrderItemsETL") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    logger.info("Defining schema for incoming order_items data")
    # Define the schema for the 'order_items' dataset.
    # This ensures schema enforcement and correct data type interpretation from the CSV.
    schema = StructType([
        StructField("id", IntegerType(), True), # Unique identifier for the order item
        StructField("order_id", IntegerType(), True), # Foreign key to the Orders table
        StructField("user_id", IntegerType(), True), # Foreign key to the Users (implied) table
        StructField("days_since_prior_order", IntegerType(), True), # Days since the user's previous order
        StructField("product_id", IntegerType(), True), # Foreign key to the Products table
        StructField("add_to_cart_order", IntegerType(), True), # Order in which product was added to cart
        StructField("reordered", IntegerType(), True), # Flag indicating if the product was reordered
        StructField("order_timestamp", StringType(), True), # Raw timestamp string from source
        StructField("date", StringType(), True) # Date string for partitioning
    ])

    # Define the S3 path for raw order_items data.
    raw_path = "s3://lab5lakehouse/lakehouse/raw/order_items/order_items.csv"
    logger.info(f"Reading raw data from {raw_path}")
    # Read the raw CSV data into a Spark DataFrame.
    # 'header=true' indicates the first row is a header.
    # 'schema' applies the predefined schema for data type consistency.
    df = spark.read.format("csv").option("header", "true").schema(schema).load(raw_path)

    logger.info("Filtering out records with nulls in critical fields")
    # Filter the DataFrame to include only valid records.
    # Records are considered valid if 'id', 'order_id', and 'product_id' are not null.
    valid_df = df.filter(
        col("id").isNotNull() &
        col("order_id").isNotNull() &
        col("product_id").isNotNull()
    )

    logger.info("Converting order_timestamp to timestamp type")
    # Convert the 'order_timestamp' column from StringType to TimestampType.
    # This ensures proper date/time operations and consistency.
    valid_df = valid_df.withColumn("order_timestamp", to_timestamp("order_timestamp"))

    logger.info("Deduplicating records")
    # Deduplicate records based on a combination of primary key fields.
    # This ensures uniqueness for each order item entry.
    deduped_df = valid_df.dropDuplicates(["id", "order_id", "user_id"])

    # Define the S3 path for the clean Delta Lake table.
    output_path = "s3://lab5lakehouse/lakehouse-dwh/order_items/"
    logger.info(f"Writing clean data to Delta format at {output_path}")
    # Write the deduplicated and cleaned data to the Delta Lake table.
    # 'format("delta")': Specifies Delta Lake format.
    # 'mode("overwrite")': Overwrites the entire table if it exists (for initial load simplicity).
    # 'partitionBy("date")': Partitions the data by the 'date' column for optimized querying.
    deduped_df.write.format("delta").mode("overwrite").partitionBy("date").save(output_path)

    logger.info("Extracting invalid rows for rejection")
    # Identify records that failed the initial validation (nulls in critical fields).
    invalid_df = df.filter(
        col("id").isNull() |
        col("order_id").isNull() |
        col("product_id").isNull()
    )

    # Define the S3 path for rejected records.
    rejected_path = "s3://lab5lakehouse/lakehouse/rejected/order_items/"
    invalid_count = invalid_df.count() # Get the count of invalid records

    # Check if there are any invalid records to write.
    if invalid_count > 0:
        logger.warning(f"Found {invalid_count} invalid records. Writing to {rejected_path}")
        # Write invalid records to the rejected zone in CSV format.
        # 'mode("overwrite")': Overwrites previous rejected files (consider 'append' for historical logs).
        # 'header=True': Includes header in the rejected CSV file.
        invalid_df.write.mode("overwrite").csv(rejected_path, header=True)
    else:
        logger.info("No invalid records found. Rejected zone will remain untouched.")

    logger.info("ETL job completed successfully.")

except Exception as e:
    # Catch any exceptions that occur during the ETL process.
    # Log the error message and traceback for debugging.
    logger.error(f"ETL job failed: {str(e)}", exc_info=True)
    sys.exit(1) # Exit with a non-zero status code to indicate failure in an automated environment

finally:
    # Ensure the SparkSession is always stopped, regardless of success or failure.
    spark.stop()
    logger.info("Spark session stopped.")

