# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a3bfc8d6-dd93-9221-4ea2-306db2ad64db",
# META       "default_lakehouse_name": "LH_250822_disable",
# META       "default_lakehouse_workspace_id": "00000000-0000-0000-0000-000000000000",
# META       "known_lakehouses": [
# META         {
# META           "id": "94441ff7-e148-a6ec-4412-7d8ffaffd72c"
# META         },
# META         {
# META           "id": "a3bfc8d6-dd93-9221-4ea2-306db2ad64db"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

df = spark.sql("SELECT * FROM LH_250822_disable.publicholidays LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM LH_250822_enable.year_2017.green_tripdata_2017 LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# abfss://Tenant2_RestrictedWSPL_01@daily-onelake.dfs.fabric.microsoft.com/LH_250822_disable.Lakehouse/Tables/publicholidays

# CELL ********************

display(spark.read.format('delta').load("abfss://Tenant2_RestrictedWSPL_01@daily-onelake.dfs.fabric.microsoft.com/LH_250822_disable.Lakehouse/Tables/publicholidays"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(spark.read.format('csv').option('header','true').load("abfss://Tenant2_RestrictedWSPL_01@daily-onelake.dfs.fabric.microsoft.com/LH_250822_disable.Lakehouse/Tables/publicholidays"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
