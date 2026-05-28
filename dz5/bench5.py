"""
Benchmark: Simple Pandas Function vs Python UDF vs Native Spark

This benchmark compares three approaches for a simple transformation:
1. Python UDF - row-by-row processing with serialization overhead
2. Pandas UDF - vectorized processing with Arrow serialization
3. Native Spark - no serialization, runs entirely in JVM
"""

import pytest
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType


@pytest.fixture(scope="session")
def spark_session():
    spark = SparkSession.builder \
        .appName("PandasVsUDFBenchmark") \
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


# =============================================================================
# Simple transformation: normalize bid_price to range [0, 1]
# Formula: (value - min) / (max - min), simplified to value / 10.0 for benchmark
# =============================================================================

# --- ❌ TEST 1: Python UDF (Slow - row-by-row serialization) ---
def test_python_udf_simple(benchmark, data):
    """
    Python UDF processes data row-by-row.
    Each row is serialized from JVM to Python and back.
    This is the SLOWEST approach.
    """
    
    def normalize_price(price):
        if price is None:
            return 0.0
        return price / 10.0
    
    normalize_udf = F.udf(normalize_price, DoubleType())
    
    def run():
        result = data.withColumn("normalized_price", normalize_udf(F.col("bid_price")))
        result.write.format("noop").mode("overwrite").save()
    
    benchmark(run)


# --- ⚡ TEST 2: Pandas UDF (Fast - vectorized with Arrow) ---
def test_pandas_udf_simple(benchmark, data):
    """
    Pandas UDF processes data in batches (chunks).
    Uses Apache Arrow for efficient serialization.
    Much faster than row-by-row Python UDF.
    """
    
    @F.pandas_udf(DoubleType())
    def normalize_pandas(prices: pd.Series) -> pd.Series:
        return prices.fillna(0.0) / 10.0
    
    def run():
        result = data.withColumn("normalized_price", normalize_pandas(F.col("bid_price")))
        result.write.format("noop").mode("overwrite").save()
    
    benchmark(run)


# --- 🚀 TEST 3: Native Spark Functions (Fastest - no serialization) ---
def test_native_spark_simple(benchmark, data):
    """
    Native Spark functions run entirely in JVM.
    No Python serialization overhead at all.
    This is the FASTEST approach.
    """
    
    def run():
        result = data.withColumn(
            "normalized_price",
            F.coalesce(F.col("bid_price"), F.lit(0.0)) / 10.0
        )
        result.write.format("noop").mode("overwrite").save()
    
    benchmark(run)


# =============================================================================
# More complex transformation: calculate discount based on multiple conditions
# =============================================================================

# --- ❌ TEST 4: Python UDF Complex ---
def test_python_udf_complex(benchmark, data):
    """
    Complex Python UDF with multiple conditions.
    """
    
    def calculate_discount(bid_price, is_premium):
        if bid_price is None:
            return 0.0
        
        base_discount = 0.0
        
        if bid_price > 5.0:
            base_discount = 0.15
        elif bid_price > 2.0:
            base_discount = 0.10
        elif bid_price > 1.0:
            base_discount = 0.05
        
        if is_premium:
            base_discount += 0.05
        
        return bid_price * (1 - base_discount)
    
    discount_udf = F.udf(calculate_discount, DoubleType())
    
    def run():
        # Create a dummy is_premium column for the benchmark
        df_with_premium = data.withColumn(
            "is_premium", 
            F.col("publisher_id") == "publisher_whale"
        )
        result = df_with_premium.withColumn(
            "discounted_price", 
            discount_udf(F.col("bid_price"), F.col("is_premium"))
        )
        result.write.format("noop").mode("overwrite").save()
    
    benchmark(run)


# --- ⚡ TEST 5: Pandas UDF Complex ---
def test_pandas_udf_complex(benchmark, data):
    """
    Complex Pandas UDF with vectorized operations.
    """
    
    @F.pandas_udf(DoubleType())
    def calculate_discount_pandas(bid_price: pd.Series, is_premium: pd.Series) -> pd.Series:
        bid_price = bid_price.fillna(0.0)
        
        # Vectorized conditions
        base_discount = pd.Series(0.0, index=bid_price.index)
        base_discount = base_discount.where(bid_price <= 1.0, 0.05)
        base_discount = base_discount.where(bid_price <= 2.0, 0.10)
        base_discount = base_discount.where(bid_price <= 5.0, 0.15)
        
        # Add premium bonus
        base_discount = base_discount + is_premium.astype(float) * 0.05
        
        return bid_price * (1 - base_discount)
    
    def run():
        df_with_premium = data.withColumn(
            "is_premium", 
            F.col("publisher_id") == "publisher_whale"
        )
        result = df_with_premium.withColumn(
            "discounted_price", 
            calculate_discount_pandas(F.col("bid_price"), F.col("is_premium"))
        )
        result.write.format("noop").mode("overwrite").save()
    
    benchmark(run)


# --- 🚀 TEST 6: Native Spark Complex ---
def test_native_spark_complex(benchmark, data):
    """
    Complex transformation using native Spark functions.
    """
    
    def run():
        df_with_premium = data.withColumn(
            "is_premium", 
            F.col("publisher_id") == "publisher_whale"
        )
        
        bid_price = F.coalesce(F.col("bid_price"), F.lit(0.0))
        
        base_discount = (
            F.when(bid_price > 5.0, 0.15)
             .when(bid_price > 2.0, 0.10)
             .when(bid_price > 1.0, 0.05)
             .otherwise(0.0)
        )
        
        premium_bonus = F.when(F.col("is_premium"), 0.05).otherwise(0.0)
        
        result = df_with_premium.withColumn(
            "discounted_price",
            bid_price * (1 - (base_discount + premium_bonus))
        )
        result.write.format("noop").mode("overwrite").save()
    
    benchmark(run)