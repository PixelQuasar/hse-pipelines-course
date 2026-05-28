import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

@pytest.fixture(scope="session")
def spark_session():
    spark = SparkSession.builder \
        .appName("PartitionBenchmark") \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    return spark

@pytest.fixture(scope="session")
def data(spark_session):
    df = spark_session.read.parquet("impressions.parquet")
    
    # Принудительно создаем 200 партиций (эмуляция "до фильтрации")
    df = df.repartition(200)
    df.cache()
    df.count()
    return df

# --- ❌ TEST 1: Too Many Partitions (Anti-Pattern) ---
def test_many_small_partitions(benchmark, data):
    
    def run_fragmented():
        # Фильтруем: оставляем только "мелких" паблишеров (~15% данных)
        # После фильтрации в 200 партициях останется очень мало данных
        filtered = data.filter(~F.col("publisher_id").isin(["publisher_whale", "publisher_medium_fish"]))
        
        # Агрегация: Spark запустит 200 микроскопических задач
        # Оверхед планировщика > полезной работы
        result = filtered.groupBy("geo_id").agg(
            F.sum("bid_price").alias("total_bid"),
            F.count("*").alias("count")
        )
        
        result.write.format("noop").mode("overwrite").save()

    benchmark(run_fragmented)

# --- ✅ TEST 2: Coalesce After Filter (Best Practice) ---
def test_coalesced_partitions(benchmark, data):
    
    def run_coalesced():
        # Та же фильтрация
        filtered = data.filter(~F.col("publisher_id").isin(["publisher_whale", "publisher_medium_fish"]))
        
        # ОПТИМИЗАЦИЯ: Уменьшаем партиции до разумного числа
        # coalesce() НЕ вызывает shuffle (в отличие от repartition)
        # Просто объединяет соседние партиции
        coalesced = filtered.coalesce(12)  # 12 партиций под 12 ядер
        
        # Теперь Spark запустит 12 нормальных задач вместо 200 микроскопических
        result = coalesced.groupBy("geo_id").agg(
            F.sum("bid_price").alias("total_bid"),
            F.count("*").alias("count")
        )
        
        result.write.format("noop").mode("overwrite").save()

    benchmark(run_coalesced)

# --- БОНУС TEST 3: Сравнение с repartition (для полноты картины) ---
def test_repartition_vs_coalesce(benchmark, data):
    
    def run_repartition():
        filtered = data.filter(~F.col("publisher_id").isin(["publisher_whale", "publisher_medium_fish"]))
        
        # repartition() ВЫЗЫВАЕТ полный shuffle (дорого!)
        # Используйте только когда нужно УВЕЛИЧИТЬ партиции или перебалансировать
        repartitioned = filtered.repartition(12)
        
        result = repartitioned.groupBy("geo_id").agg(
            F.sum("bid_price").alias("total_bid"),
            F.count("*").alias("count")
        )
        
        result.write.format("noop").mode("overwrite").save()

    benchmark(run_repartition)
