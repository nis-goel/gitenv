# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "5fbc5c0c-f2cf-8ea2-442a-2f75934f3d5e",
# META       "default_lakehouse_name": "another_test",
# META       "default_lakehouse_workspace_id": "00000000-0000-0000-0000-000000000000",
# META       "known_lakehouses": [
# META         {
# META           "id": "5fbc5c0c-f2cf-8ea2-442a-2f75934f3d5e"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
1+1

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.format("csv").option("header","true").load("abfss://455500f8-03b3-405c-8c36-b9f2bfde11b1@daily-onelake.dfs.fabric.microsoft.com/934f3d5e-2f75-442a-8ea2-f2cf5fbc5c0c/Files/sample_datasets/city_safety_seattle.csv")
# df now is a Spark DataFrame containing CSV data from "abfss://455500f8-03b3-405c-8c36-b9f2bfde11b1@daily-onelake.dfs.fabric.microsoft.com/934f3d5e-2f75-442a-8ea2-f2cf5fbc5c0c/Files/sample_datasets/city_safety_seattle.csv".
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.format("csv").option("header","true").load("Files/sample_datasets/city_safety_seattle.csv")
# df now is a Spark DataFrame containing CSV data from "Files/sample_datasets/city_safety_seattle.csv".
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM another_test.publicholidays LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
