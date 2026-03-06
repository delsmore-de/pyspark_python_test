from pyspark.sql import SparkSession, DataFrame
import os
from .transformations import SimplifyColumnNames

class examplePipeline:
    def __init__(self, spark_session: SparkSession):
        self.spark_session = spark_session
        self.transformations = [SimplifyColumnNames()]

    def read_csv_file(self):
        "Reads a CSV file a returns a spark dataframe"
        path = os.path.join("data", "iris.csv")
        return self.spark_session.read.format("csv").option("header", "true").load(path)

    def read_parquet(self):
        "Reads the given parquet file and returns a dataframe"
        path = os.path.join("data", "irisparquet")
        return self.spark_session.read.format('parquet').options(header=True,inferSchema=True).load(path)

    def run_transformations(self, df: DataFrame):
        "runs all transformations registered to the pipeline"
        for transform in self.transformations:
            df = transform.run(df)
        return df

    def write_file(self, df: DataFrame):
        '''
        method for write the final dataframe output to a file

        left as an exercise for the reader
        '''
        pass

    def run(self):
        df = self.read_parquet()
        df = self.run_transformations(df)
        df.show(5)
        return df