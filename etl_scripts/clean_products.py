from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import logging
import sys

# Configure structured logging for better visibility and debugging during job execution.
logging.basicConfig(
    level=logging.INFO, # Set the logging level to INFO to capture general progress and important messages.
    format="%(asctime)s [%(levelname)s] %(message)s" # Define the format for log messages (timestamp, level, message).
)
logger = logging.getLogger() # Get the root logger instance to use throughout the script.

# Function to filter valid rows and deduplicate product data.
def clean_products_data(df: DataFrame) -> DataFrame:
    logger.info("Filtering valid product records...")
    # Filter the DataFrame to keep only valid records.
    # A record is considered valid if 'product_id' and 'product_name' are not null.
    valid_df = df.filter(col("product_id").isNotNull() & col("product_name").isNotNull())
    
    logger.info("Removing duplicate products by product_id...")
    # Deduplicate records based on 'product_id'.
    # In case of duplicate 'product_id's, the first occurrence is kept.
    deduped_df = valid_df.dropDuplicates(["product_id"])
    return deduped_df

# Function to extract invalid rows for rejection.
def filter_invalid_rows(df: DataFrame) -> DataFrame:
    logger.info("Extracting invalid rows with null product_id or product_name...")
    # Identify records that have null values in 'product_id' or 'product_name'.
    # These records will be moved to the rejected zone.
    return df.filter(col("product_id").isNull() | col("product_name").isNull())

# Main function to orchestrate the ETL process for products data.
def main():
    try:
        logger.info("Starting Spark session with Delta Lake support...")
        # Initialize SparkSession with necessary configurations for Delta Lake.
        # .appName(): Sets a name for the Spark application.
        # .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"): Enables Delta Lake SQL features.
        # .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"): Configures Spark to use Delta Lake's catalog.
        # .getOrCreate(): Gets an existing SparkSession or creates a new one.
        spark = SparkSession.builder \
            .appName("CleanProductsETL") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()

        logger.info("Defining schema for products.csv...")
        # Define the explicit schema for the 'products' dataset.
        # This ensures schema enforcement and correct data type interpretation from the CSV.
        schema = StructType([
            StructField("product_id", IntegerType(), True), # Unique identifier for the product.
            StructField("department_id", IntegerType(), True), # Identifier for the department the product belongs to.
            StructField("department", StringType(), True), # Name of the department.
            StructField("product_name", StringType(), True) # Name of the product.
        ])

        # Define the S3 path where the raw 'products.csv' data is located.
        raw_path = "s3://lab5lakehouse/lakehouse/raw/products/products.csv"
        logger.info(f"Reading raw data from {raw_path}")
        # Read the raw CSV data into a Spark DataFrame.
        # .option("header", "true"): Indicates that the first row is a header.
        # .schema(schema): Applies the predefined schema for data type consistency.
        df = spark.read.format("csv").option("header", "true").schema(schema).load(raw_path)

        logger.info("Running cleaning and validation functions...")
        # Call the clean_products_data function to filter valid records and deduplicate.
        clean_df = clean_products_data(df)
        # Define the S3 path where the cleaned and processed Delta Lake table for products will be stored.
        clean_output_path = "s3://lab5lakehouse/lakehouse-dwh/products/"
        logger.info(f"Writing clean data to {clean_output_path}")
        # Write the cleaned DataFrame to the specified S3 path in Delta Lake format.
        # .format("delta"): Specifies the output format as Delta Lake.
        # .mode("overwrite"): Overwrites the entire Delta table if it already exists (suitable for initial loads).
        clean_df.write.format("delta").mode("overwrite").save(clean_output_path)

        # Call the filter_invalid_rows function to identify and extract invalid records.
        invalid_df = filter_invalid_rows(df)
        # Define the S3 path for storing rejected (invalid) records.
        rejected_path = "s3://lab5lakehouse/lakehouse/rejected/products/"
        invalid_count = invalid_df.count() # Count the number of invalid records found.

        # Check if there are any invalid records to write.
        if invalid_count > 0:
            logger.warning(f"{invalid_count} invalid records found. Writing to {rejected_path}")
            # Write the invalid records to the rejected zone in CSV format.
            # 'mode("overwrite")': Overwrites any previously rejected files in this path.
            # 'header=True': Includes column headers in the output CSV file.
            invalid_df.write.mode("overwrite").csv(rejected_path, header=True)
        else:
            logger.info("No invalid product records found.") # Log if no invalid records were found.

        logger.info("ETL job for products completed successfully.")

    except Exception as e:
        # Catch any exceptions that occur during the ETL process.
        # Log the error message and include traceback information for detailed debugging.
        logger.error(f"ETL job failed: {str(e)}", exc_info=True)
        sys.exit(1) # Exit the script with a non-zero status code to indicate failure to the orchestrator.

    finally:
        # This block ensures that the SparkSession is always stopped,
        # regardless of whether the job succeeded or failed, to release resources.
        spark.stop()
        logger.info("Spark session stopped.")

# Entry point for the script execution.
if __name__ == "__main__":
    main()
