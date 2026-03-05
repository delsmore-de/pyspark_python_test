from pyspark.sql import SparkSession, DataFrame
from utils.spark_helper import create_delta_spark_session, stop_spark_session
import os

class example_pipeline:
    def __init__(self):
        self.spark_session = create_delta_spark_session()

    def read_csv_file(self):
        "Reads a CSV file a returns a spark dataframe"
        path = os.path.join("data", "iris.csv")
        return self.spark_session.read.format("csv").option("header", "true").load(path)

    def read_parquet(self):
        "Reads the given parquet file and returns a dataframe"
        path = os.path.join("data", "irisparquet")
        return self.spark_session.read.format('parquet').options(header=True,inferSchema=True).load(path)

    def transformations(self, df: DataFrame):

        pass

    def write_file(self, df: DataFrame):
        
        pass

    def run(self):

        df = self.read_parquet()
        df.show()

    def stop(self):
        stop_spark_session(self.spark_session)

if __name__=="__main__":
    pipeline = example_pipeline()
    try:
        pipeline.run()
    except Exception as e:
        print(e)
    finally:
        pipeline.stop()