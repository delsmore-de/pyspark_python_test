from pyspark.sql import SparkSession
from utils.spark_helper import create_delta_spark_session, stop_spark_session

def main():
    spark_session = create_delta_spark_session()

    df = spark_session.createDataFrame([("Hello, world!",)], ["text"])
    df.show()

    stop_spark_session(spark_session)

if __name__=="__main__":
    print("running")
    main()