from pyspark.sql import SparkSession
from delta.pip_utils import configure_spark_with_delta_pip
import os
import shutil
import tempfile

def warehouse_path():
    cwd = os.path.abspath(os.path.dirname(__file__))
    temp_dir = os.path.join(cwd, "spark_temp")
    return temp_dir

def create_delta_spark_session():
    """Creates a SparkSession with Delta Lake support. This session is configured to use a temporary warehouse directory for Delta Lake storage."""
    temp_dir = warehouse_path()
    os.makedirs(temp_dir, exist_ok=True)
    warehouse_dir = tempfile.mkdtemp(dir=temp_dir)

    builder = (
        SparkSession.builder.appName("TestSpark")
        .master("local[*]") #create local session; * for # of cpu cores as your machine has
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.3.1") #enables deltatable for the spark session locally
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.spark_catalog.warehousePath", warehouse_dir)
    )

    spark_session = configure_spark_with_delta_pip(builder).getOrCreate()

    return spark_session

def stop_spark_session(spark_session):
    """Stops the given SparkSession and cleans up the temporary warehouse directory used for Delta Lake storage."""
    spark_session.stop()
    shutil.rmtree(warehouse_path(), ignore_errors=True)