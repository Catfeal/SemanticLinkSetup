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

# CELL ********************

%run Fnc_StartupFabricSempy

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

prm_starttime = datetime.now()
NotebookName = "Nbt_Landing_GetSMHistory"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

Table_Name = 'Landing_Fabric_SM_RefreshHistory'

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

# CELL ********************

SemanticModels = spark.sql("""select Id, WSID
from Lkh_SemanticLink.Landing_Fabric_Items
where Type='SemanticModel' and DisplayName<>'Report Usage Metrics Model'""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for Id, WSID in SemanticModels.toLocalIterator():
    dataset_ID = Id
    try:
        a = sempy_labs.get_semantic_model_refresh_history(dataset = dataset_ID, workspace = WSID)     
        df = pd.DataFrame(a)
        if not df.empty: # check if the list is not empty to avoid errors
            df = pd.DataFrame(a)
            df.drop('Attempts', inplace=True, axis=1)
            df = fnc_PrepareColumns(df)
            sparkdf = spark.createDataFrame(df)
            sparkdf = sparkdf.withColumn('WSID', lit(WSID))
            sparkdf = sparkdf.withColumn('SMID', lit(dataset_ID))
            sparkdf.write.format("delta").option("mergeSchema", "true").mode("append").save(f"{lh_abfs_path}/Tables/{Table_Name}")                   
    except Exception as e:
        print(f"Error fetching semantic model objects for {dataset_ID}: {e}")
        continue

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#fill list of workspaces
workspaces = fabric.list_workspaces().sort_values(by='Name', ascending=True)
df = pd.DataFrame(workspaces)

# loop through the workspaces
for label, content in df.items():
    if label == 'Id': #loop through the ID's
        for id in content:
            #lijst vullen met alle items in de werkruimte
            temp_items = fabric.list_items(workspace=id)
            itemdf = pd.DataFrame(temp_items)
            #print(itemdf)
            if not itemdf.empty: # check if the list is not empty to avoid errors
            # filter out semantic models
                df_semantic_models = itemdf[(itemdf['Type'] == 'SemanticModel') & (itemdf['Display Name'] != 'Report Usage Metrics Model')]
                #print(df_semantic_models)
                for _, row in df_semantic_models.iterrows():
                    dataset_name = row['Display Name']
                    dataset_ID = row['Id']
                    #get the relationships of each model
                    try:
                        a = sempy_labs.get_semantic_model_refresh_history(dataset = dataset_ID, workspace = id)     
                        df = pd.DataFrame(a)
                        if not df.empty: # check if the list is not empty to avoid errors
                            df = pd.DataFrame(a)
                            df.drop('Attempts', inplace=True, axis=1)
                            df = fnc_PrepareColumns(df)
                            sparkdf = spark.createDataFrame(df)
                            sparkdf = sparkdf.withColumn('WSID', lit(id))
                            sparkdf = sparkdf.withColumn('SMID', lit(dataset_ID))
                            sparkdf.write.format("delta").option("mergeSchema", "true").mode("append").save(f"{lh_abfs_path}/Tables/{Table_Name}")                   
                    except Exception as e:
                        print(f"Error fetching semantic model objects for {dataset_name}: {e}")
                        continue



                    

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

prm_endtime = datetime.utcnow()
fnc_LognotebookRefresh(NotebookName, prm_starttime, prm_endtime)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
