import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, LongType
import time

@pytest.fixture(scope="session")
def spark_session():
    spark = SparkSession.builder \
        .appName("DataFrameVsRDDBenchmark") \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    return spark

@pytest.fixture(scope="session")
def data(spark_session):
    # Read data from dz5 folder
    df = spark_session.read.parquet("dz5/impressions.parquet")
    # Ensure we have enough data for meaningful benchmarks
    df = df.limit(20_000)
    df.cache()
    df.count() # Force cache
    return df

# =============================================================================
# CASE 1: Multiple Aggregations
# DataFrame uses HashAggregate with code generation (Tungsten).
# RDD requires manual mapping and reducing, often involving Python overhead or inefficient shuffling.
# =============================================================================

def test_case1_dataframe_agg(benchmark, data):
    """
    DataFrame API: Optimized HashAggregate.
    Catalyst combines all aggregations into a single pass.
    """
    def run():
        result = data.groupBy("publisher_id").agg(
            F.sum("bid_price").alias("total_bid"),
            F.avg("bid_price").alias("avg_bid"),
            F.min("bid_price").alias("min_bid"),
            F.max("bid_price").alias("max_bid"),
            F.count("bid_price").alias("count_bid")
        )
        result.collect()
    
    benchmark(run)

def test_case1_rdd_agg(benchmark, data):
    """
    RDD API: Manual map + reduceByKey.
    Much slower due to:
    1. Python serialization overhead (if using PySpark RDD)
    2. Lack of Tungsten binary format (objects overhead)
    3. No code generation
    """
    rdd = data.rdd
    
    def run():
        # Map: key -> (sum, count, min, max, sum_sq for avg? let's keep it simple: sum, count, min, max)
        # We need to handle the tuple structure manually
        def map_func(row):
            price = row.bid_price if row.bid_price is not None else 0.0
            return (row.publisher_id, (price, 1, price, price))

        def reduce_func(a, b):
            return (
                a[0] + b[0], # sum
                a[1] + b[1], # count
                min(a[2], b[2]), # min
                max(a[3], b[3])  # max
            )
            
        result = rdd.map(map_func).reduceByKey(reduce_func).collect()
        
        # Post-processing to get avg would happen here, but we stop at collect for fair comparison of "heavy lifting"
    
    benchmark(run)


# =============================================================================
# CASE 2: Window Functions (Top-N per group)
# DataFrame uses optimized WindowExec.
# RDD requires groupByKey (OOM risk) or repartitionAndSortWithinPartitions.
# =============================================================================

def test_case2_dataframe_window(benchmark, data):
    """
    DataFrame API: Window function.
    Catalyst optimizes the sort and rank operation.
    """
    def run():
        windowSpec = Window.partitionBy("publisher_id").orderBy(F.col("bid_price").desc())
        result = data.withColumn("rank", F.rank().over(windowSpec)) \
                     .filter(F.col("rank") <= 5)
        result.write.format("noop").mode("overwrite").save()
    
    benchmark(run)

def test_case2_rdd_window(benchmark, data):
    """
    RDD API: GroupBy + Sort in Python.
    Extremely inefficient because groupByKey shuffles all data for a key to one executor.
    """
    rdd = data.rdd
    
    def run():
        def get_top_n(iterable):
            # Sort in memory for each group
            sorted_items = sorted(iterable, key=lambda x: x.bid_price if x.bid_price else 0.0, reverse=True)
            return sorted_items[:5]

        # groupByKey is notoriously slow and dangerous
        # Using mapPartitions to avoid serialization issues with large objects if possible, but sticking to simple logic for bench
        result = rdd.groupBy(lambda x: x.publisher_id) \
                    .mapValues(get_top_n) \
                    .collect()
    
    benchmark(run)


# =============================================================================
# CASE 3: Conditional Logic (when/otherwise vs filter+union)
# DataFrame generates a single projection with CaseWhen expression.
# RDD/Manual approach might use multiple passes or complex python logic.
# =============================================================================

def test_case3_dataframe_conditional(benchmark, data):
    """
    DataFrame API: when/otherwise.
    Compiles to a single ProjectExec with CaseWhen expression.
    """
    def run():
        result = data.withColumn(
            "price_category",
            F.when(F.col("bid_price") > 100, "High")
             .when(F.col("bid_price") > 50, "Medium")
             .when(F.col("bid_price") > 10, "Low")
             .otherwise("Tiny")
        )
        result.write.format("noop").mode("overwrite").save()
    
    benchmark(run)

def test_case3_rdd_conditional(benchmark, data):
    """
    RDD API: Python lambda with if/else.
    Slow due to serialization and Python execution overhead per row.
    """
    rdd = data.rdd
    
    def run():
        def categorize(row):
            price = row.bid_price if row.bid_price is not None else 0.0
            if price > 100:
                cat = "High"
            elif price > 50:
                cat = "Medium"
            elif price > 10:
                cat = "Low"
            else:
                cat = "Tiny"
            return (row.transaction_id, cat)
            
        result = rdd.map(categorize).collect()
    
    benchmark(run)
