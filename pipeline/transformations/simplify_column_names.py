from .transformation_base import TransformationBase
from pyspark.sql import DataFrame, functions as F
import re

class SimplifyColumnNames(TransformationBase):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
    
    def run(self, df: DataFrame):
        df = df.select([F.col("`{0}`".format(c)).alias(c.replace('.', '')) for c in df.columns])
        return df
