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
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, TimestampType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

prm_starttime = datetime.utcnow()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # code

# CELL ********************

def fnc_startUp(_Tablename: StringType):
    """
    Parameters:
        _Tablename = he name of the table you want to use in this part of the code
    Actions in the function
        - check if it exists and truncate/drop if it does
        - Create the notebookrefresh table so we can build our history for that
        - get the path of the lakehouse 
    """

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

# MARKDOWN ********************

# # set global variables

# CELL ********************

def fnc_TableCheckStartOfRun(_Tablename, _TruncateOrDrop):
    """
    Parameters:

        _Tablename = the table we are going to use

        _TruncateOrDrop = we can drop or Truncate the table, options to provide (Truncate or Drop)
    """
    if spark.catalog.tableExists(_Tablename):
        if(_TruncateOrDrop == 'Truncate'):
            spark.sql(f'TRUNCATE table {_Tablename}')
        if(_TruncateOrDrop == 'Delete'):
            spark.sql(f'DROP table {_Tablename}')


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

def fnc_LognotebookRefresh(_NotebookName: StringType, _StartTime: datetime, _EndTime: datetime):
    spark.sql(f"INSERT INTO refreshTimes_Notebooks (NotebookName, StartTime, EndTime) VALUES ('{_NotebookName}', '{_StartTime}', '{_EndTime}')")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def fnc_WriteToTable(_TableDataFrame):
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
