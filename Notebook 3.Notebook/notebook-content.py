# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

%run Fnc_StartupFabricSempy

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

WorkspaceId = fabric.get_workspace_id()
LakehouseName = "Lkh_Testing_FromDifferenetNotebook"
tablename= "test_table"
#LH_Abs_Path = fnc_startUp("table name", LakehouseName, WorkspaceId)
LH_Abs_Path = fnc_startUp(tablename, LakehouseName, WorkspaceId)
print(LH_Abs_Path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

lake_path = os.getenv("ABS_LAKE_PATH")
print(lake_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fnc_LognotebookRefresh(LH_Abs_Path, WorkspaceId, "name", datetime.now(), datetime.now())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os
import pandas as pd

workspaceID = fabric.get_workspace_id()
_LH_Name = "Lkh_Testing_FromDifferenetNotebook"
lakehouse = mssparkutils.lakehouse.get(_LH_Name)
lh_abfs_path = lakehouse.get("properties").get("abfsPath")

base_path = lh_abfs_path
mssparkutils.fs.mount(base_path, mount_name)
mount_points = mssparkutils.fs.mounts()
local_path = next((mp["localPath"] for mp in mount_points if mp["mountPoint"] == mount_name), None)
print('localpath', local_path)


#print(os.path.exists(local_path)) #check if location exists
#print(os.listdir(local_path + "/Files")) # for files
print(os.listdir(local_path + "/Tables")) # for tables
#df = pd.read_csv(local_path + "/Tables/"+ "")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

tables = os.listdir(local_path + "/Tables")
for t in tables:
    print(t)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
