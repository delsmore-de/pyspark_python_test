from abc import abstractmethod
from pyspark.sql import SparkSession, DataFrame

class TransformationBase():
    def __init__(self, **kwargs):
        self.kwargs = kwargs
    
    @abstractmethod
    def run(df: DataFrame) -> DataFrame:
        raise NotImplementedError
