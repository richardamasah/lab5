from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

def clean_products_data(df: DataFrame) -> DataFrame:
    # ✅ Filter valid rows
    valid_df = df.filter(col("product_id").isNotNull() & col("product_name").isNotNull())
    # ✅ Deduplicate
    deduped_df = valid_df.dropDuplicates(["product_id"])
    return deduped_df

def filter_invalid_rows(df: DataFrame) -> DataFrame:
    return df.filter(col("product_id").isNull() | col("product_name").isNull())

def main():
    spark = SparkSession.builder \
        .appName("CleanProductsETL") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    schema = StructType([
        StructField("product_id", IntegerType(), True),
        StructField("department_id", IntegerType(), True),
        StructField("department", StringType(), True),
        StructField("product_name", StringType(), True)
    ])

    raw_path = "s3://lab5lakehouse/lakehouse/raw/products/products.csv"
    df = spark.read.format("csv").option("header", "true").schema(schema).load(raw_path)

    clean_df = clean_products_data(df)
    clean_df.write.format("delta").mode("overwrite").save("s3://lab5lakehouse/lakehouse-dwh/products/")

    invalid_df = filter_invalid_rows(df)
    if invalid_df.count() > 0:
        invalid_df.write.mode("overwrite").csv("s3://lab5lakehouse/lakehouse/rejected/products/", header=True)

    spark.stop()

if __name__ == "__main__":
    main()
