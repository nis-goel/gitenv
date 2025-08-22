import sys
from pyspark.sql import SparkSession

if __name__ == "__main__":

    #Spark session builder
    spark_session = (SparkSession
          .builder
          .appName("ConvertToDeltaApp")
          .config("spark.some.config.option", "some-value")
          .getOrCreate())
    
    spark_context = spark_session.sparkContext
    spark_context.setLogLevel("DEBUG")

    #Import data from csvFilePath to deltaTablePath
    lakehouseArtifactId = "09def35c-a502-4f98-8732-4edee92dd9ad"
    tableName = "NycTaxiSmall"
    csvFilePath = "Files/sample_datasets/city_safety_seattle.csv"
    deltaTablePath = "Tables/" + tableName

    df = spark_session.read.format('csv').options(header='true', inferschema='true').load(csvFilePath)
    df.write.mode('overwrite').format('delta').save(deltaTablePath)