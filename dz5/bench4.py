import pytest
import json
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, BooleanType, DoubleType

@pytest.fixture(scope="session")
def spark_session():
    spark = SparkSession.builder \
        .appName("SerializationBenchmark") \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    return spark

@pytest.fixture(scope="session")
def data(spark_session):
    df = spark_session.read.parquet("impressions.parquet")
    df = df.limit(500_000)
    df.cache()
    df.count()
    return df

def test_python_udf(benchmark, data):
    def extract_device(ua):
        if ua is None:
            return "unknown"
        if "iPhone" in ua or "iPad" in ua:
            return "ios"
        elif "Android" in ua:
            return "android"
        else:
            return "other"
    
    def is_internal_ip(ip):
        if ip is None:
            return False
        return ip.startswith("192.168.") or ip.startswith("10.")
    
    def extract_floor_price(json_str):
        if json_str is None:
            return 0.0
        try:
            return float(json.loads(json_str).get("floor_price", 0.0))
        except:
            return 0.0
    
    device_udf = F.udf(extract_device, StringType())
    internal_udf = F.udf(is_internal_ip, BooleanType())
    floor_udf = F.udf(extract_floor_price, DoubleType())
    
    def run():
        result = data \
            .withColumn("device_type", device_udf(F.col("user_agent"))) \
            .withColumn("is_internal", internal_udf(F.col("ip_address"))) \
            .withColumn("floor_price", floor_udf(F.col("bid_request")))
        result.write.format("noop").mode("overwrite").save()
    
    benchmark(run)

def test_native_functions(benchmark, data):
    def run():
        result = data \
            .withColumn(
                "device_type",
                F.when(F.col("user_agent").isNull(), "unknown")
                 .when(F.col("user_agent").contains("iPhone") | F.col("user_agent").contains("iPad"), "ios")
                 .when(F.col("user_agent").contains("Android"), "android")
                 .otherwise("other")
            ) \
            .withColumn(
                "is_internal",
                F.col("ip_address").startswith("192.168.") | F.col("ip_address").startswith("10.")
            ) \
            .withColumn(
                "floor_price",
                F.get_json_object(F.col("bid_request"), "$.floor_price").cast(DoubleType())
            )
        result.write.format("noop").mode("overwrite").save()
    
    benchmark(run)
