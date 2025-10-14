# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

import sempy.fabric as fabric
import sempy_labs as sempy_labs
from sempy_labs import migration, report, directlake
from sempy_labs import lakehouse as lake
from sempy_labs.tom import connect_semantic_model
import pandas as pd
import pyspark.sql.functions
from delta.tables import DeltaTable
from pyspark.sql.functions import lit, current_timestamp
from datetime import datetime
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, TimestampType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import sempy.fabric as fabric
from sempy.fabric.exceptions import FabricHTTPException

lh_name = "Lkh_SemanticLinkData2"
ws_name = None #if None use the default workspace else use the specified
ws_name = '4c390f3e-c1ef-48fe-b264-830181ea210c'
try:
    lh = fabric.create_lakehouse(display_name=lh_name, workspace=ws_name)
    print(f"Lakehouse {lh_name} with ID {lh} successfully created")

except FabricHTTPException as exc:
    print(f"An error occurred: {exc}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Get the current workspace
WorkspaceID = notebookutils.runtime.context["currentWorkspaceId"]
display(WorkspaceID)
# Get the name of the default lakehouse
DefaultLakehouseName = notebookutils.runtime.context["defaultLakehouseName"]
print("LakehouseName")
print(DefaultLakehouseName)
# Get the id of the same lakehouse in the new workspace
LakehouseID = notebookutils.lakehouse.get(DefaultLakehouseName, WorkspaceID)["id"]

# Get the connection string to the new Lakehouse
LakehousePath = f"abfss://{WorkspaceID}@onelake.dfs.fabric.microsoft.com/{LakehouseID}"

# Mount the new lakehouse
notebookutils.fs.mount( 
 f"abfss://{WorkspaceID}@onelake.dfs.fabric.microsoft.com/{LakehouseID}", 
 "/autoMount"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Define Lakehouse name and description.
LH_Name = "SemanticLink_Lakehouse"
LH_Name = "Lkh_SemanticLink"
LH_desc = "Lakehouse for Power BI usage monitoring"

# Setup log table voor refrhes opvolging van de notebooks
fnc_NotebookRefreshTableCheck() 

# Truncate or drop the table to have a clear start to insert into  
fnc_TableCheckStartOfRun(_Tablename, 'Truncate')

# Mount the Lakehouse for direct file system access
lakehouse = mssparkutils.lakehouse.get(LH_Name)
mssparkutils.fs.mount(lakehouse.get("properties").get("abfsPath"), f"/{LH_Name}")

# Retrieve and store local and ABFS paths of the mounted Lakehouse
local_path = mssparkutils.fs.getMountPath(f"/{LH_Name}")
lh_abfs_path = lakehouse.get("properties").get("abfsPath")
return lh_abfs_path

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
