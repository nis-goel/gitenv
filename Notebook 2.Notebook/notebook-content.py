# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "224d96b7-4c73-99cd-47fc-563b2290b75e",
# META       "default_lakehouse_name": "Lakehouse01",
# META       "default_lakehouse_workspace_id": "00000000-0000-0000-0000-000000000000",
# META       "known_lakehouses": [
# META         {
# META           "id": "67f7bba0-6712-a236-4a6c-d8baed004d03"
# META         },
# META         {
# META           "id": "224d96b7-4c73-99cd-47fc-563b2290b75e"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************


# CELL ********************

df = spark.read.option("multiline", "true").json("Files/General.json")
# df now is a Spark DataFrame containing JSON data from "Files/General.json".
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
