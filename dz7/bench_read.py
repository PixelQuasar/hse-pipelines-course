import pytest
import time

@pytest.fixture(scope="module")
def prepared_data(spark_session, datasets):
    df = datasets["impressions"]
    
    import os
    
    # Delta
    delta_path = os.path.abspath("dz7/warehouse/delta_read")
    df.write.format("delta").mode("overwrite").save(delta_path)
    
    # Iceberg
    spark_session.sql("DROP TABLE IF EXISTS iceberg.default.impressions_read")
    df.writeTo("iceberg.default.impressions_read").create()
    
    # Hudi
    hudi_path = os.path.abspath("dz7/warehouse/hudi_read")
    hudi_options = {
        'hoodie.table.name': 'hudi_read',
        'hoodie.datasource.write.recordkey.field': 'impression_id',
        'hoodie.datasource.write.precombine.field': 'timestamp',
        'hoodie.datasource.write.operation': 'insert'
    }
    df.write.format("hudi").options(**hudi_options).mode("overwrite").save(hudi_path)
    
    return True

@pytest.mark.benchmark(group="read_performance")
def test_delta_read(benchmark, spark_session, prepared_data):
    import os
    delta_path = os.path.abspath("dz7/warehouse/delta_read")
    def run():
        spark_session.read.format("delta").load(delta_path).filter("bid_price > 0.5").count()
    benchmark(run)

@pytest.mark.benchmark(group="read_performance")
def test_iceberg_read(benchmark, spark_session, prepared_data):
    def run():
        spark_session.read.format("iceberg").load("iceberg.default.impressions_read").filter("bid_price > 0.5").count()
    benchmark(run)

@pytest.mark.benchmark(group="read_performance")
def test_hudi_read(benchmark, spark_session, prepared_data):
    import os
    hudi_path = os.path.abspath("dz7/warehouse/hudi_read")
    def run():
        spark_session.read.format("hudi").load(hudi_path).filter("bid_price > 0.5").count()
    benchmark(run)