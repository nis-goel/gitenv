# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "environment": {
# META       "environmentId": "3f7d237c-922c-99d8-43ac-23f06c452d43",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************

1+1


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import fuzzywuzzy
print("fuzzywuzzy:{}".format(fuzzywuzzy.__version__))

import scipy
print("scipy:{}".format(scipy.__version__))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%spark
# MAGIC import com.vdurmont.emoji._;
# MAGIC val str = "An :grinning:awesome :smiley:string &#128516;with a few :wink:emojis!";
# MAGIC print(EmojiParser.parseToUnicode(str));

# METADATA ********************

# META {
# META   "language": "scala",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sparkr
# MAGIC library(adlift)

# METADATA ********************

# META {
# META   "language": "r",
# META   "language_group": "synapse_pyspark"
# META }
