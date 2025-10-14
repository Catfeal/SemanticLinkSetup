# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

%pip install semantic-link-labs

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
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
from sempy.fabric.exceptions import FabricHTTPException
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, TimestampType
import numpy as np
import os

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

WS_ID = fabric.get_workspace_id()
_LH_Name = "Lkh_Test"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fnc_SET_PATH_abs_LH(WS_ID, _LH_Name)
fnc_SET_PATH_LocalMount()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


%%configure -f

{ 
        "defaultLakehouse": { 
            "name":  "Lkh_Test"
            }
    }
     

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # code

# CELL ********************

def fnc_startUp(_Tablename: StringType, _LH_Name: StringType, _WS_ID: StringType):
    """
    This function will do all the initial setups for the Semantic link flow

    - Check for lakehouse and create if necessary

    - check for table in that lakehouse and truncate or drop 
    """

    fnc_SET_PATH_abs_LH(_WS_ID, _LH_Name)

    fnc_SET_PATH_LocalMount()

    

    # Truncate or drop the table to have a clear start to insert into  
    fnc_TableCheckStartOfRun(_Tablename, 'Truncate')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # set global variables

# CELL ********************

def fnc_SET_PATH_abs_LH(_WorkspaceID: StringType, _LH_Name: StringType):
    """
    This function will check if the specified lakehouse exists and create it if not
    """
    Items = fabric.list_items(workspace=_WorkspaceID)
    FileInWS = Items[(Items['Type'] == 'Lakehouse') & (Items['Display Name'] == _LH_Name)]
    if(FileInWS.empty):
        try:
            lh = fabric.create_lakehouse(display_name=_LH_Name, workspace=_WorkspaceID)
            print(f"Lakehouse {_LH_Name} with ID {lh} successfully created")
        except FabricHTTPException as exc:
            print(f"An error occurred: {exc}")
    # Mount the Lakehouse for direct file system access
    lakehouse = mssparkutils.lakehouse.get(_LH_Name)
    mssparkutils.fs.mount(lakehouse.get("properties").get("abfsPath"), f"/{_LH_Name}")
    lh_abfs_path = lakehouse.get("properties").get("abfsPath")
    os.environ["ABS_LAKE_PATH"] = lh_abfs_path

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def fnc_SET_PATH_LocalMount():
    mount_name = '/temp_mnt'
    base_path = os.getenv("ABS_LAKE_PATH")
    mssparkutils.fs.mount(base_path, mount_name)
    mount_points = mssparkutils.fs.mounts()
    local_path = next((mp["localPath"] for mp in mount_points if mp["mountPoint"] == mount_name), None)
    os.environ["Mount_LAKE_PATH"] = local_path

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # initial setups

# CELL ********************

def fnc_TableCheckStartOfRun(_Tablename, _TruncateOrDrop):
    """
    Parameters:

        _Tablename = the table we are going to use

        _TruncateOrDrop = we can drop or Truncate the table, options to provide (Truncate or Drop)
    """
    lh_abfs_path = os.getenv("Mount_LAKE_PATH")
    notebookname = f"{lh_abfs_path}/Tables/{_Tablename}"

    if spark.catalog.tableExists(notebookname):
        if(_TruncateOrDrop == 'Truncate'):
            spark.sql(f'TRUNCATE table {_Tablename}')
        if(_TruncateOrDrop == 'Delete'):
            spark.sql(f'DROP table {_Tablename}')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def fnc_NotebookRefreshTableCheck():
    lh_abfs_path = os.getenv("ABS_LAKE_PATH")
    lh_local_path = os.getenv("Mount_LAKE_PATH")
    tablename = "refreshTimes_Notebooks"
    tables = os.listdir(lh_local_path + "/Tables")
    exists = False
    for t in tables:
        if(t == tablename):
            exists=True
    print(exists)
    #if spark.catalog.tableExists(f"{lh_abfs_path}/Tables/{tableName}"):
    if(exists):
        print('refreshTimes_Notebooks Existed')
    else:    
        schema = StructType([
            StructField("NotebookName", StringType(), False),
            StructField("StartTime", TimestampType(), True),
            StructField("EndTime", TimestampType(), True)
        ])
        df = spark.createDataFrame([], schema)
        notebookname = f"{lh_abfs_path}/Tables/refreshTimes_Notebooks"
        print(notebookname)
        df.write.format("delta").save(notebookname)
        #df.write.format("delta").saveAsTable(f"{lh_abfs_path}/Tables/refreshTimes_Notebooks") 
        #df.write.format("delta").saveAsTable(f"{lh_abfs_path}/Tables/{tablename}") 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# MARKDOWN ********************

# # functions to call

# CELL ********************

def fnc_PrepareColumns(_Columns):
    _Columns.columns = _Columns.columns.str.replace('[^a-zA-Z0-9]', '', regex=True)
    _Columns.columns = _Columns.columns.str.replace('[ ]', '', regex=True)
    return _Columns

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def fnc_LognotebookRefresh(workspace_id, _NotebookName: StringType, _StartTime: datetime, _EndTime: datetime):
    lh_abfs_path = os.getenv("ABS_LAKE_PATH")
    Table_Name = "refreshTimes_Notebooks"
    d = spark.createDataFrame([(_NotebookName, _StartTime, _EndTime)],["NotebookName", "StartTime", "EndTime"])
    print(d)
    #df = spark.createDataFrame(data=d)
    d.write.format("delta").mode("append").option("mergeSchema", "true").save(f"{lh_abfs_path}/Tables/{Table_Name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def fnc_WriteToTable(_TableDataFrame, lh_abfs_path):
    """
    This function accepts a dataframe, changes the column names to something a lakehouse will accept and then save it
    """
    try:
        PreparedDataframe = fnc_PrepareColumns(_TableDataFrame)
        sparkdf = spark.createDataFrame(PreparedDataframe)
        sparkdf = sparkdf.withColumn('WSID', lit(Id))
        sparkdf.write.format("delta").option("mergeSchema", "true").mode("append").save(f"{lh_abfs_path}/Tables/{Table_Name}")
    except Exception as e:
        print(f"Error fetching Workspace objects for {Id}: {e}")
        

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
