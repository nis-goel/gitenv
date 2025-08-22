import sys
from pyspark.sql import SparkSession

if __name__ == "__main__":

    #Spark session builder
    spark_session = (SparkSession
          .builder
          .appName("OnePlusOneApp")
          .config("spark.some.config.option", "some-value")
          .getOrCreate())
    
    spark_context = spark_session.sparkContext
    spark_context.setLogLevel("DEBUG")

    #Comoute 1+1
    df = 1+1
    print(df)