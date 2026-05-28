import pytest
import threading
import time
from pyspark.sql import SparkSession
import os
import shutil

def run_concurrent_write(spark_session, table_path, format_name, options=None):
    """
    Helper to run concurrent writes.
    """
    results = []
    
    def write_job(job_id):
        try:
            # Create a new SparkSession for each thread to simulate concurrent users
            # Note: In local mode, they share the same JVM, but this simulates separate jobs
            spark = spark_session.newSession()
            
            data = [(job_id, f"val_{job_id}")]
            df = spark.createDataFrame(data, ["id", "value"])
            
            writer = df.write.format(format_name).mode("append")
            if options:
                writer = writer.options(**options)
            
            if format_name == "iceberg":
                 df.writeTo(table_path).append()
            else:
                writer.save(table_path)
                
            results.append((job_id, "Success"))
        except Exception as e:
            results.append((job_id, f"Failed: {str(e)}"))

    threads = []
    for i in range(2):
        t = threading.Thread(target=write_job, args=(i,))
        threads.append(t)
        t.start()
        
    for t in threads:
        t.join()
        
    return results

def test_concurrent_delta(spark_session):
    path = "dz7/warehouse/delta_concurrent"
    if os.path.exists(path):
        shutil.rmtree(path)
        
    # Initialize table
    spark_session.createDataFrame([(0, "init")], ["id", "value"]).write.format("delta").save(path)
    
    results = run_concurrent_write(spark_session, path, "delta")
    print("\nDelta Concurrent Results:", results)
    
    # Verify data
    count = spark_session.read.format("delta").load(path).count()
    print(f"Delta Final Count: {count}")
    # Delta supports optimistic concurrency, so both might succeed if no conflict on same files
    # But here we are appending, so it should be fine.
    # To test conflict, we would need to update same row.

def test_concurrent_iceberg(spark_session):
    table_name = "iceberg.default.concurrent"
    spark_session.sql(f"DROP TABLE IF EXISTS {table_name}")
    
    # Initialize table
    spark_session.createDataFrame([(0, "init")], ["id", "value"]).writeTo(table_name).create()
    
    results = run_concurrent_write(spark_session, table_name, "iceberg")
    print("\nIceberg Concurrent Results:", results)
    
    count = spark_session.table(table_name).count()
    print(f"Iceberg Final Count: {count}")

def test_concurrent_hudi(spark_session):
    path = os.path.abspath("dz7/warehouse/hudi_concurrent")
    if os.path.exists(path):
        shutil.rmtree(path)
    
    hudi_options = {
        'hoodie.table.name': 'hudi_concurrent',
        'hoodie.datasource.write.recordkey.field': 'id',
        'hoodie.datasource.write.precombine.field': 'id', # simple precombine
        'hoodie.datasource.write.operation': 'insert',
        'hoodie.cleaner.policy': 'KEEP_LATEST_COMMITS',
        'hoodie.cleaner.commits.retained': 1
    }
    
    # Initialize table
    spark_session.createDataFrame([(0, "init")], ["id", "value"]).write.format("hudi").options(**hudi_options).save(path)
    
    results = run_concurrent_write(spark_session, path, "hudi", hudi_options)
    print("\nHudi Concurrent Results:", results)
    
    count = spark_session.read.format("hudi").load(path).count()
    print(f"Hudi Final Count: {count}")
