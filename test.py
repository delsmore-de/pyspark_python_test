from pyspark.sql import SparkSession

def main():
    print("test")
    spark = SparkSession.builder.appName("HelloWorld").getOrCreate()
    df = spark.createDataFrame([("Hello, world!",)], ["text"])
    df.show()

if __name__=="__main__":
    print("running")
    main()