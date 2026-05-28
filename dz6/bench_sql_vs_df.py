import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

@pytest.fixture(scope="session")
def spark_session():
    spark = SparkSession.builder \
        .appName("SQLVsDataFrameBenchmark") \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    return spark

@pytest.fixture(scope="session")
def data(spark_session):
    df = spark_session.read.parquet("dz5/impressions.parquet")
    df = df.limit(20_000)
    df.createOrReplaceTempView("impressions")
    df.cache()
    df.count()
    return df

# =============================================================================
# CASE 1: Complex Filtering with Subqueries/Joins
# SQL optimizers are often better at pushing down predicates and reordering joins
# when expressed declaratively in SQL compared to imperative DataFrame chains.
# =============================================================================

def test_sql_complex_filter(benchmark, spark_session, data):
    """
    SQL Query: Often faster because the Catalyst optimizer can see the whole picture
    and reorder operations more effectively than when using DataFrame API step-by-step.
    """
    def run():
        query = """
        SELECT publisher_id, count(*) as cnt
        FROM impressions
        WHERE bid_price > (SELECT avg(bid_price) FROM impressions)
        GROUP BY publisher_id
        HAVING cnt > 100
        """
        spark_session.sql(query).collect()
    
    benchmark(run)

def test_df_complex_filter(benchmark, data):
    """
    DataFrame API: Equivalent logic but constructed imperatively.
    Sometimes this can lead to less optimal plans if not careful, 
    though modern Catalyst is very good at optimizing both.
    """
    def run():
        avg_price = data.select(F.avg("bid_price")).collect()[0][0]
        result = data.filter(F.col("bid_price") > avg_price) \
                     .groupBy("publisher_id") \
                     .count() \
                     .filter(F.col("count") > 100)
        result.collect()
    
    benchmark(run)

# =============================================================================
# CASE 2: Case-Insensitive String Operations
# SQL is often more concise and sometimes optimized better for standard SQL patterns.
# =============================================================================

def test_sql_string_ops(benchmark, spark_session, data):
    """
    SQL Query: Using standard SQL string functions.
    """
    def run():
        query = """
        SELECT 
            lower(publisher_id) as pub_lower,
            sum(bid_price) as total_bid
        FROM impressions
        WHERE publisher_id LIKE 'pub_%'
        GROUP BY lower(publisher_id)
        """
        spark_session.sql(query).collect()
    
    benchmark(run)

def test_df_string_ops(benchmark, data):
    """
    DataFrame API: Using PySpark functions.
    """
    def run():
        result = data.filter(F.col("publisher_id").startswith("pub_")) \
                     .groupBy(F.lower(F.col("publisher_id")).alias("pub_lower")) \
                     .agg(F.sum("bid_price").alias("total_bid"))
        result.collect()
    
    benchmark(run)