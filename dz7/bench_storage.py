import pytest
import os
import shutil

def get_dir_size(path):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            if not os.path.islink(fp):
                total_size += os.path.getsize(fp)
    return total_size

@pytest.mark.benchmark(group="storage_size")
def test_delta_size(spark_session, datasets):
    df = datasets["impressions"]
    path = "dz7/warehouse/delta_table"
    if os.path.exists(path):
        shutil.rmtree(path)
        
    df.write.format("delta").save(path)
    
    size = get_dir_size(path)
    print(f"Delta Lake Size: {size / 1024 / 1024:.2f} MB")
    assert size > 0

@pytest.mark.benchmark(group="storage_size")
def test_iceberg_size(spark_session, datasets):
    df = datasets["impressions"]
    table_name = "iceberg.default.impressions"
    spark_session.sql(f"DROP TABLE IF EXISTS {table_name}")
    
    df.writeTo(table_name).create()
    
    # Iceberg stores data in warehouse/iceberg/default/impressions
    path = "dz7/warehouse/iceberg/default/impressions"
    size = get_dir_size(path)
    print(f"Iceberg Size: {size / 1024 / 1024:.2f} MB")
    assert size > 0

@pytest.mark.benchmark(group="storage_size")
def test_hudi_size(spark_session, datasets):
    df = datasets["impressions"]
    path = os.path.abspath("dz7/warehouse/hudi_table")
    if os.path.exists(path):
        shutil.rmtree(path)
        
    hudi_options = {
        'hoodie.table.name': 'hudi_table',
        'hoodie.datasource.write.recordkey.field': 'impression_id',
        'hoodie.datasource.write.precombine.field': 'timestamp',
        'hoodie.datasource.write.operation': 'insert'
    }
    
    df.write.format("hudi").options(**hudi_options).save(path)
    
    size = get_dir_size(path)
    print(f"Hudi Size: {size / 1024 / 1024:.2f} MB")
    assert size > 0