# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

try:
    notebookutils.lakehouse.create(name = "FUAM_Config_Lakehouse")
except Exception as ex:
    print('Lakehouse already exists')
     

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%configure -f
# MAGIC 
# MAGIC { 
# MAGIC         "defaultLakehouse": { 
# MAGIC             "name":  "FUAM_Config_Lakehouse"
# MAGIC             }
# MAGIC     }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
