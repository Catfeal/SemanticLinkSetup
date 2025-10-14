# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "060fba99-17e2-4946-b182-3ab820ed66f8",
# META       "default_lakehouse_name": "Lkh_SemanticLink",
# META       "default_lakehouse_workspace_id": "1c0073d5-1ff1-4af1-9a1d-7f57255eb849",
# META       "known_lakehouses": [
# META         {
# META           "id": "060fba99-17e2-4946-b182-3ab820ed66f8"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# De benamingen voor de tabellen die we gaan vullen

# CELL ********************

%run Fnc_StartupFabricSempy

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

prm_starttime = datetime.now()
NotebookName = "Nbt_Landing_GetWorkspaces"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

Table_Name = 'Landing_Fabric_Workspaces'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

lh_abfs_path = fnc_startUp(Table_Name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Alle benodigde lijsten om de code vlot te laten lopen

# CELL ********************

#fill list of workspaces
workspaces = fabric.list_workspaces()
workspaces
workspaces = fnc_PrepareColumns(workspaces)
sparkdf = spark.createDataFrame(workspaces)
sparkdf.write.format("delta").option("mergeSchema", "true").mode("overwrite").save(f"{lh_abfs_path}/Tables/{Table_Name}")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

prm_endtime = datetime.utcnow()
fnc_LognotebookRefresh(NotebookName, prm_starttime, prm_endtime)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
