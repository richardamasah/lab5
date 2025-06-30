import pytest
from pyspark.sql import SparkSession
from etl_scripts.clean_products import clean_products_data, filter_invalid_rows

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local").appName("TestSession").getOrCreate()

def test_clean_products_data(spark):
    df = spark.createDataFrame([
        (1, 10, "grocery", "apple"),
        (2, 20, "snacks", None),
        (None, 30, "drinks", "juice"),
        (1, 10, "grocery", "apple")  # duplicate
    ], ["product_id", "department_id", "department", "product_name"])

    clean_df = clean_products_data(df)

    assert clean_df.count() == 1
    row = clean_df.collect()[0]
    assert row.product_id == 1
    assert row.product_name == "apple"

def test_filter_invalid_rows(spark):
    df = spark.createDataFrame([
        (None, 10, "snacks", "banana"),
        (2, 20, "frozen", None),
        (3, 30, "meat", "beef")
    ], ["product_id", "department_id", "department", "product_name"])

    invalid_df = filter_invalid_rows(df)
    assert invalid_df.count() == 2
