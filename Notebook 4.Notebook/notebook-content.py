# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

from notebookutils import mssparkutils
_LH_Name = "Lkh_Testing_FromDifferenetNotebook"
lakehouse = mssparkutils.lakehouse.get(_LH_Name)
mssparkutils.fs.mount(lakehouse.get("properties").get("abfsPath"), f"/{_LH_Name}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(mssparkutils.lakehouse.list())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("select * from Lkh_Testing_FromDifferenetNotebook.refreshTimes_Notebooks").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
