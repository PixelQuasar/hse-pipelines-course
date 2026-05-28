import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

@pytest.fixture(scope="session")
def spark_session():
    spark = SparkSession.builder \
        .appName("Benchmark") \
        .master("local[*]") \
        .config("spark.driver.memory", "8g") \
        .config("spark.executor.memory", "8g") \
        .config("spark.memory.fraction", "0.6") \
        .config("spark.memory.storageFraction", "0.3") \
        .config("spark.ui.showConsoleProgress", "false") \
        .config("spark.sql.autoBroadcastJoinThreshold", -1) \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    return spark

@pytest.fixture(scope="session")
def dataframes(spark_session):
    # Читаем данные БЕЗ кэширования - чтобы shuffle был честным
    # Используем 10M строк - компромисс между нагрузкой и OOM
    df_imp = spark_session.read.parquet("impressions.parquet").limit(10_000_000)
    df_geo = spark_session.read.parquet("geo_dict.parquet")
    
    # Кэшируем только маленький справочник
    df_geo.cache()
    df_geo.count()
    
    return df_imp, df_geo

def test_shuffle_join(benchmark, dataframes):
    df_imp, df_geo = dataframes
    
    def run_shuffle_join():    
        joined = df_imp.join(df_geo, "geo_id")
        joined.write.format("noop").mode("overwrite").save()

    benchmark(run_shuffle_join)

def test_broadcast_join(benchmark, dataframes):
    df_imp, df_geo = dataframes
    
    def run_broadcast_join():
        joined = df_imp.join(broadcast(df_geo), "geo_id")
        joined.write.format("noop").mode("overwrite").save()

    benchmark(run_broadcast_join)
