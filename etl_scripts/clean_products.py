from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# ✅ Initialize SparkSession with Delta Lake support
spark = SparkSession.builder \
    .appName("CleanProductsETL") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# ✅ Define schema
schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("department_id", IntegerType(), True),
    StructField("department", StringType(), True),
    StructField("product_name", StringType(), True)
])

# ✅ Read raw CSV from S3
raw_path = "s3://lab5lakehouse/lakehouse/raw/products/products.csv"
df = spark.read.format("csv").option("header", "true").schema(schema).load(raw_path)

# ✅ Validation: Filter out bad rows (null product_id or product_name)
valid_df = df.filter(col("product_id").isNotNull() & col("product_name").isNotNull())

# ✅ Deduplicate by product_id
deduped_df = valid_df.dropDuplicates(["product_id"])

# ✅ Write clean data to Delta format
clean_output_path = "s3://lab5lakehouse/lakehouse-dwh/products/"
deduped_df.write.format("delta").mode("overwrite").save(clean_output_path)

# ✅ Write invalid rows to rejected zone
invalid_df = df.filter(col("product_id").isNull() | col("product_name").isNull())
rejected_path = "s3://lab5lakehouse/lakehouse/rejected/products/"
if invalid_df.count() > 0:
    invalid_df.write.mode("overwrite").csv(rejected_path, header=True)

spark.stop()
