import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

@pytest.fixture(scope="session")
def spark_session():
    spark = SparkSession.builder \
        .appName("SerializationBenchmark") \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    return spark

@pytest.fixture(scope="session")
def data(spark_session):
    df = spark_session.read.parquet("impressions.parquet")
    # Берем срез для ускорения бенчмарка
    df = df.limit(500_000)
    df.cache()
    df.count()
    return df

# --- ❌ TEST 1: Python UDF (Serialization Hell) ---
def test_python_udf(benchmark, data, spark_session):
    
    # Бизнес-логика: категоризация bid_price
    def categorize_bid(price):
        if price is None:
            return "unknown"
        elif price == 0:
            return "zero"
        elif price < 2.0:
            return "low"
        elif price < 5.0:
            return "medium"
        else:
            return "high"
    
    # Регистрируем UDF
    categorize_udf = F.udf(categorize_bid, StringType())
    
    def run_udf():
        result = data.withColumn("bid_category", categorize_udf(F.col("bid_price")))
        result.write.format("noop").mode("overwrite").save()

    benchmark(run_udf)

# --- ✅ TEST 2: Native Spark Functions (No Serialization) ---
def test_native_functions(benchmark, data):
    
    def run_native():
        # Та же логика, но через встроенные функции Spark
        result = data.withColumn(
            "bid_category",
            F.when(F.col("bid_price").isNull(), "unknown")
             .when(F.col("bid_price") == 0, "zero")
             .when(F.col("bid_price") < 2.0, "low")
             .when(F.col("bid_price") < 5.0, "medium")
             .otherwise("high")
        )
        result.write.format("noop").mode("overwrite").save()

    benchmark(run_native)

# --- БОНУС TEST 3: Pandas UDF (Vectorized - компромисс) ---
def test_pandas_udf(benchmark, data, spark_session):
    
    import pandas as pd
    
    @F.pandas_udf(StringType())
    def categorize_pandas(prices: pd.Series) -> pd.Series:
        # Векторизованная операция - обрабатываем batch, а не по одной строке
        return pd.cut(
            prices.fillna(-1),
            bins=[-float('inf'), 0, 0.001, 2.0, 5.0, float('inf')],
            labels=["unknown", "zero", "low", "medium", "high"]
        ).astype(str)
    
    def run_pandas_udf():
        result = data.withColumn("bid_category", categorize_pandas(F.col("bid_price")))
        result.write.format("noop").mode("overwrite").save()

    benchmark(run_pandas_udf)
