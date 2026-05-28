import pytest
import shutil
import os

@pytest.mark.benchmark(group="write_performance")
def test_delta_write(benchmark, spark_session, datasets):
    df = datasets["impressions"]
    path = "dz7/warehouse/delta_write"
    
    def run():
        if os.path.exists(path):
            shutil.rmtree(path)
        df.write.format("delta").save(path)
        
    benchmark(run)

@pytest.mark.benchmark(group="write_performance")
def test_iceberg_write(benchmark, spark_session, datasets):
    df = datasets["impressions"]
    table_name = "iceberg.default.impressions_write"
    
    def run():
        spark_session.sql(f"DROP TABLE IF EXISTS {table_name}")
        df.writeTo(table_name).create()
        
    benchmark(run)

@pytest.mark.benchmark(group="write_performance")
def test_hudi_write(benchmark, spark_session, datasets):
    df = datasets["impressions"]
    path = os.path.abspath("dz7/warehouse/hudi_write")
    
    hudi_options = {
        'hoodie.table.name': 'hudi_write',
        'hoodie.datasource.write.recordkey.field': 'impression_id',
        'hoodie.datasource.write.precombine.field': 'timestamp',
        'hoodie.datasource.write.operation': 'insert'
    }
    
    def run():
        if os.path.exists(path):
            shutil.rmtree(path)
        df.write.format("hudi").options(**hudi_options).save(path)
        
    benchmark(run)