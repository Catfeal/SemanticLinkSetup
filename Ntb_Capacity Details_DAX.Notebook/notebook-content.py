# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "58b60afe-756a-4ef1-ad34-3b1068ae2081",
# META       "default_lakehouse_name": "Lkh_CapacityReportData",
# META       "default_lakehouse_workspace_id": "1c0073d5-1ff1-4af1-9a1d-7f57255eb849",
# META       "known_lakehouses": [
# META         {
# META           "id": "58b60afe-756a-4ef1-ad34-3b1068ae2081"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

%run Fnc_StartupFabricSempy

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

prm_starttime = datetime.now()
NotebookName = "Nbt_Capacty Details_DAX"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def fnc_Startup_DAX(_tableName):
    fnc_startUp(_tableName)
    LH_Name = "Lkh_CapacityReportData"
    lakehouse = mssparkutils.lakehouse.get(LH_Name)
    lh_abfs_path = lakehouse.get("properties").get("abfsPath")
    return lh_abfs_path

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Items ophalen en wegschrijven

# CELL ********************

Table_Name = 'landing_Items'
lh_abfs_path = fnc_Startup_DAX(Table_Name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = sempy_labs.evaluate_dax_impersonation(
dataset = 'DataVanFabriCapacityReport',
dax_query = """EVALUATE
SUMMARIZE(
			'Items',
			'Items'[Item],
			'Items'[ItemId],
			'Items'[ItemKind],
			'Items'[ItemName],
			'Items'[WorkspaceId],
			'Items'[capacityId],
			'Items'[Billable type]
        )
		""" )
df = fnc_PrepareColumns(df)
sparkdf = spark.createDataFrame(df)
sparkdf.write.format("delta").option("mergeSchema", "true").mode("append").save(f"{lh_abfs_path}/Tables/{Table_Name}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Capacities ophalen en wegschrijven

# CELL ********************

Table_Name = 'landing_Capacities'
lh_abfs_path = fnc_startUp(Table_Name)
LH_Name = "Lkh_CapacityReportData"
lakehouse = mssparkutils.lakehouse.get(LH_Name)
lh_abfs_path = lakehouse.get("properties").get("abfsPath")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = sempy_labs.evaluate_dax_impersonation(
dataset = 'DataVanFabriCapacityReport',
dax_query = """EVALUATE
SUMMARIZE(
			'Capacities',
			'Capacities'[capacityId],
			'Capacities'[Capacity Name],
			'Capacities'[capacityMemoryInGB],
			'Capacities'[capacityNumberOfVCores],
			'Capacities'[capacityPlan],
			'Capacities'[creationDate],
			'Capacities'[Owners],
			'Capacities'[region],
			'Capacities'[sku]
		)
		""" )
df = fnc_PrepareColumns(df)
sparkdf = spark.createDataFrame(df)
sparkdf.write.format("delta").option("mergeSchema", "true").mode("append").save(f"{lh_abfs_path}/Tables/{Table_Name}")




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Interactive details

# CELL ********************

Table_Name = 'landing_InteractiveDetails'
lh_abfs_path = fnc_startUp(Table_Name)
LH_Name = "Lkh_CapacityReportData"
lakehouse = mssparkutils.lakehouse.get(LH_Name)
lh_abfs_path = lakehouse.get("properties").get("abfsPath")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = sempy_labs.evaluate_dax_impersonation(
dataset = 'DataVanFabriCapacityReport',
dax_query = """EVALUATE
		SUMMARIZE(
			'InteractiveDetail',
			'InteractiveDetail'[ItemId],
			'InteractiveDetail'[CUs],
			'InteractiveDetail'[Duration (s)],
			'InteractiveDetail'[OperationName],
			'InteractiveDetail'[OperationStartTime],
			'InteractiveDetail'[OperationEndTime],
			'InteractiveDetail'[Status],
			'InteractiveDetail'[Throttling (s)],
			'InteractiveDetail'[Key]
		)
		""" )
df = fnc_PrepareColumns(df)
sparkdf = spark.createDataFrame(df)
sparkdf.write.format("delta").option("mergeSchema", "true").mode("append").save(f"{lh_abfs_path}/Tables/{Table_Name}")




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Background details

# CELL ********************

Table_Name = 'landing_BackgroundDetails'
lh_abfs_path = fnc_startUp(Table_Name)
LH_Name = "Lkh_CapacityReportData"
lakehouse = mssparkutils.lakehouse.get(LH_Name)
lh_abfs_path = lakehouse.get("properties").get("abfsPath")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = sempy_labs.evaluate_dax_impersonation(
dataset = 'DataVanFabriCapacityReport',
dax_query = """EVALUATE
		SUMMARIZE(
			'BackgroundDetail',
			'BackgroundDetail'[Key],
			'BackgroundDetail'[ItemId],
			'BackgroundDetail'[Duration (s)],
			'BackgroundDetail'[CUs],
			'BackgroundDetail'[OperationEndTime],
			'BackgroundDetail'[OperationName],
			'BackgroundDetail'[OperationStartTime],
			'BackgroundDetail'[Status],
			'BackgroundDetail'[Throttling (s)]
		)
		""" )
df = fnc_PrepareColumns(df)
sparkdf = spark.createDataFrame(df)
sparkdf.write.format("delta").option("mergeSchema", "true").mode("append").save(f"{lh_abfs_path}/Tables/{Table_Name}")




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## storage by workspace and day

# CELL ********************

Table_Name = 'landing_StorageByWorkspaceAndDay'
lh_abfs_path = fnc_Startup_DAX(Table_Name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = sempy_labs.evaluate_dax_impersonation(
dataset = 'DataVanFabriCapacityReport',
dax_query = """EVALUATE
        SUMMARIZE(
			'StorageByWorkspacesandDay',
			'StorageByWorkspacesandDay'[WorkspaceId],
			'StorageByWorkspacesandDay'[OperationName],
			'StorageByWorkspacesandDay'[Hours],
			'StorageByWorkspacesandDay'[StaticStorageInGb],
			'StorageByWorkspacesandDay'[Utilization (GB)],
			'StorageByWorkspacesandDay'[Date],
			'StorageByWorkspacesandDay'[WorkspaceKey],
			'StorageByWorkspacesandDay'[WorkloadKind],
			'StorageByWorkspacesandDay'[PremiumCapacityId],
			'StorageByWorkspacesandDay'[Billing type]
		)   
		""" )
df = fnc_PrepareColumns(df)
sparkdf = spark.createDataFrame(df)
sparkdf.write.format("delta").option("mergeSchema", "true").mode("append").save(f"{lh_abfs_path}/Tables/{Table_Name}")




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## workspaces

# CELL ********************

Table_Name = 'landing_Workspaces'
lh_abfs_path = fnc_Startup_DAX(Table_Name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = sempy_labs.evaluate_dax_impersonation(
dataset = 'DataVanFabriCapacityReport',
dax_query = """EVALUATE
		SUMMARIZE(
			'Workspaces',
			'Workspaces'[PremiumCapacityId],
			'Workspaces'[WorkspaceId],
			'Workspaces'[WorkspaceKey],
			'Workspaces'[WorkspaceName],
			'Workspaces'[WorkspaceProvisionState]
		)   
		""" )
df = fnc_PrepareColumns(df)
sparkdf = spark.createDataFrame(df)
sparkdf.write.format("delta").option("mergeSchema", "true").mode("append").save(f"{lh_abfs_path}/Tables/{Table_Name}")




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## afronden

# CELL ********************

prm_endtime = datetime.utcnow()
fnc_LognotebookRefresh(NotebookName, prm_starttime, prm_endtime)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
