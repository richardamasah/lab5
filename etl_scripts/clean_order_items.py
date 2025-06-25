from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# ✅ Initialize SparkSession with Delta Lake support
spark = SparkSession.builder \
    .appName("CleanOrderItemsETL") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# ✅ Define schema
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("order_id", IntegerType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("days_since_prior_order", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("add_to_cart_order", IntegerType(), True),
    StructField("reordered", IntegerType(), True),
    StructField("order_timestamp", StringType(), True),
    StructField("date", StringType(), True)
])

# ✅ Read from S3
raw_path = "s3://lab5lakehouse/lakehouse/raw/order_items/order_items.csv"
df = spark.read.format("csv").option("header", "true").schema(schema).load(raw_path)

# ✅ Drop null critical fields
valid_df = df.filter(
    col("id").isNotNull() &
    col("order_id").isNotNull() &
    col("product_id").isNotNull()
)

# ✅ Convert timestamp
valid_df = valid_df.withColumn("order_timestamp", to_timestamp("order_timestamp"))

# ✅ Deduplicate
deduped_df = valid_df.dropDuplicates(["id", "order_id", "user_id"])

# ✅ Write to Delta (partition by date)
output_path = "s3://lab5lakehouse/lakehouse-dwh/order_items/"
deduped_df.write.format("delta").mode("overwrite").partitionBy("date").save(output_path)

# ✅ Log bad records
invalid_df = df.filter(
    col("id").isNull() | 
    col("order_id").isNull() |
    col("product_id").isNull()
)
rejected_path = "s3://lab5lakehouse/lakehouse/rejected/order_items/"
if invalid_df.count() > 0:
    invalid_df.write.mode("overwrite").csv(rejected_path, header=True)

spark.stop()
