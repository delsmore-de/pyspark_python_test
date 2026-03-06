from pipeline.example_pipeline import examplePipeline
from pyspark.sql import types as T
import pytest
from unittest.mock import patch
from utils.spark_helper import create_delta_spark_session

@pytest.fixture
def spark_session():
    return create_delta_spark_session()

@pytest.fixture
def input_schema():
    fields = [
        T.StructField("sepal.length", T.StringType(), True),
        T.StructField("sepal.width", T.StringType(), True),
        T.StructField("petal.length", T.StringType(), True),
        T.StructField("petal.width", T.StringType(), True),
        T.StructField("variety", T.StringType(), True)
    ]
    return T.StructType(fields)

def test_transform(spark_session, input_schema):
    '''
    An exmpale pipeline transformation test.  It runs a created dataframe through the pipelines run_transformation method and
    checks that the datasource names were renamed without a '.' and that the sepal lenght column is rounded to the nearest whole number
    '''
    data = [
        ('1.1', '2.0', '1.1', '0.2', 'test'),
        ('1.0', '2.0', '1.0', '.2', 'test1'),
        ('1.5', '2.0', '1.0', '.2', 'test1'),
        ('9.4', '2.0', '1.0', '.2', 'test1'),
        ('9.6', '2.0', '1.0', '.2', 'test1'),
    ]
    df = spark_session.createDataFrame(data, input_schema)
    pipeline = examplePipeline(spark_session)

    transformed_df = pipeline.run_transformations(df)

    transformed_column_names = [
        "sepallength",
        "sepalwidth",
        "petalwidth",
        "petallength",
        "variety"
    ]

    for col in transformed_column_names:
        assert col in transformed_df.columns
    
    sepallength_expected = [
        '1',
        '1',
        '2',
        '9',
        '10',
    ]
    
    for index, row in enumerate(transformed_df.collect()):
        assert row['sepallength'] == sepallength_expected[index]

