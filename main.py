from pipeline.example_pipeline import examplePipeline
from utils.spark_helper import create_delta_spark_session, stop_spark_session

def main():
    spark_session = create_delta_spark_session()
    pipeline = examplePipeline(spark_session)
    try:
        pipeline.run()
    except Exception as e:
        print(e)
    finally:
        stop_spark_session(spark_session)

if __name__=="__main__":
    main()