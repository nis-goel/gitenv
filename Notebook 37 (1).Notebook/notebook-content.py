# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# MAGIC %%configure -f 
# MAGIC {
# MAGIC     "conf": {
# MAGIC         "spark.yarn.dist.jars": "abfss://nbpltestzhang20250411@daily-onelake.dfs.fabric.microsoft.com/lh.Lakehouse/Files/notebook-utils-1.1.10-spark35-20250415.3.jar",
# MAGIC         "spark.driver.extraClassPath": "./notebook-utils-1.1.10-spark35-20250415.3.jar",
# MAGIC         "spark.executor.extraClassPath": "./notebook-utils-1.1.10-spark35-20250415.3.jar"
# MAGIC     }
# MAGIC }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%pip install /lakehouse/default/Files/notebookutils-1.1.10.35.20250415.3-py3-none-any.whl

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pip show notebookutils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ls /opt/spark/jars | grep trident-core

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

cat /opt/health-agent/conf/cluster-info.json | jq

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

cat /opt/olcclient/olcconfig_*.json | jq '.FeatureFlags'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    items = notebookutils.lakehouse.list()
    lhid = ''
    lhname = ''
    lhpath = ''
    if items:
        lhid = items[0]['id']
        lhname = items[0]['displayName']
        lhpath = items[0]['properties']['abfsPath']
        print(lhid, lhname, lhpath)
    else:
        print(items)
except:
    print(traceback.format_exc())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

notebookutils.fs.mounts()

notebookutils.fs.mount(lhpath, "/mnt")

notebookutils.fs.mounts()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

notebookutils.fs.getMountPath("/mnt")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

notebookutils.fs.unmount("/mnt")

notebookutils.fs.mounts()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if lhid:
    print(notebookutils.fs.put(lhpath + "/Files/2.txt", "abcdefg", True))
    print(notebookutils.fs.cp(lhpath+"/Files/2.txt",lhpath+"/Files/newdir/2.txt"))
    print(notebookutils.fs.fastcp(lhpath+"/Files/2.txt",lhpath+"/Files/newdir2/2.txt"))
    print(notebookutils.fs.rm(lhpath + "/Files/newdir/2.txt"))
    print(notebookutils.fs.exists(lhpath + "/Files/newdir/2.txt"))
    print(notebookutils.fs.mv(lhpath+"/Files/newdir2/2.txt",lhpath+"/Files/newdir"))
    print(notebookutils.fs.ls(lhpath+"/Files"))
    print(notebookutils.fs.mkdirs(lhpath+"/Files/newdir4"))
    print(notebookutils.fs.head(lhpath + "/Files/2.txt"))
    print(notebookutils.fs.append(lhpath + "/Files/2.txt", "hijklmn", True))
    print(notebookutils.fs.head(lhpath + "/Files/2.txt"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
