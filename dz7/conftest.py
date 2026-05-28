import pytest
from pyspark.sql import SparkSession
import os
import shutil

@pytest.fixture(scope="session")
def spark_session():
    """
    Configures SparkSession with support for Delta Lake, Iceberg, and Hudi.
    """
    warehouse_dir = os.path.abspath("dz7/warehouse")
    
    # Clean up warehouse before starting
    if os.path.exists(warehouse_dir):
        shutil.rmtree(warehouse_dir)
    
    packages = [
        "io.delta:delta-spark_2.12:3.0.0",
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0",
        "org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0"
    ]
    
    spark = SparkSession.builder \
        .appName("LakehouseBenchmark") \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.jars.packages", ",".join(packages)) \
        .config("spark.sql.warehouse.dir", warehouse_dir) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.iceberg.type", "hadoop") \
        .config("spark.sql.catalog.iceberg.warehouse", f"{warehouse_dir}/iceberg") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    yield spark
    
    spark.stop()

@pytest.fixture(scope="session")
def datasets(spark_session):
    """
    Reads the source datasets.
    """
    impressions = spark_session.read.parquet("dz7/impressions.parquet").limit(100000)
    # Add impression_id if it doesn't exist (it seems it's missing in the source data)
    if "impression_id" not in impressions.columns:
        from pyspark.sql.functions import monotonically_increasing_id
        impressions = impressions.withColumn("impression_id", monotonically_increasing_id().cast("string"))
    geo_dict = spark_session.read.parquet("dz7/geo_dict.parquet")
    
    return {
        "impressions": impressions,
        "geo_dict": geo_dict
    }