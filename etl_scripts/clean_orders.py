from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType, TimestampType

# ✅ Initialize SparkSession with Delta support
spark = SparkSession.builder \
    .appName("CleanOrdersETL") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# ✅ Define schema
schema = StructType([
    StructField("order_num", IntegerType(), True),
    StructField("order_id", IntegerType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("order_timestamp", StringType(), True),
    StructField("total_amount", FloatType(), True),
    StructField("date", StringType(), True)
])

# ✅ Read raw CSV from S3
raw_path = "s3://lab5lakehouse/lakehouse/raw/orders/orders.csv"
df = spark.read.format("csv").option("header", "true").schema(schema).load(raw_path)

# ✅ Validation: drop null order_id or user_id
valid_df = df.filter(col("order_id").isNotNull() & col("user_id").isNotNull())

# ✅ Parse order_timestamp to real timestamp
valid_df = valid_df.withColumn("order_timestamp", to_timestamp("order_timestamp"))

# ✅ Deduplicate based on order_id + order_timestamp
deduped_df = valid_df.dropDuplicates(["order_id", "order_timestamp"])

# ✅ Write to Delta Lake (partitioned by date)
clean_output_path = "s3://lab5lakehouse/lakehouse-dwh/orders/"
deduped_df.write.format("delta").mode("overwrite").partitionBy("date").save(clean_output_path)

# ✅ Log rejected rows
invalid_df = df.filter(col("order_id").isNull() | col("user_id").isNull())
rejected_path = "s3://lab5lakehouse/lakehouse/rejected/orders/"
if invalid_df.count() > 0:
    invalid_df.write.mode("overwrite").csv(rejected_path, header=True)

spark.stop()
