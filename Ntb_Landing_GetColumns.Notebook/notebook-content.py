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

# # Settings

# CELL ********************

%run Fnc_StartupFabricSempy

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

prm_starttime = datetime.now()
NotebookName = "Nbt_Landing_GetColumns"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

Table_Name = 'Landing_Fabric_Columns'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Deze functie dient om de single quotes in text velden op te vangen en te veranderen voor we problemen krijgen met dit naar de SQL tables te schrijven.

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
        columns = fabric.list_columns(dataset=dataset_ID, workspace=WSID, extended=True)
        columns.drop('Alternate Of Base Column', inplace=True, axis=1)
        columns.drop('Alternate Of Base Table', inplace=True, axis=1)
    except Exception as e:
        print(f"Error fetching semantic model objects for {dataset_ID}: {e}")
        continue
    columnsdf = pd.DataFrame(columns)
    if not columnsdf.empty: # check if the list is not empty to avoid errors
        columnsdf = fnc_PrepareColumns(columnsdf)
        sparkdf = spark.createDataFrame(columnsdf)
        sparkdf = sparkdf.withColumn('WSID', lit(WSID))
        sparkdf = sparkdf.withColumn('SMID', lit(dataset_ID))
        sparkdf.write.format("delta").option("mergeSchema", "true").mode("append").save(f"{lh_abfs_path}/Tables/{Table_Name}")

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
                    #get the measures of each model
                    try:
                        columns = fabric.list_columns(dataset=dataset_ID, workspace=id, extended=True)
                        columns.drop('Alternate Of Base Column', inplace=True, axis=1)
                        columns.drop('Alternate Of Base Table', inplace=True, axis=1)
                    except Exception as e:
                        print(f"Error fetching semantic model objects for {dataset_name}: {e}")
                        continue
                    columnsdf = pd.DataFrame(columns)
                    if not columnsdf.empty: # check if the list is not empty to avoid errors
                        columnsdf = fnc_PrepareColumns(columnsdf)
                        sparkdf = spark.createDataFrame(columnsdf)
                        sparkdf = sparkdf.withColumn('WSID', lit(id))
                        sparkdf = sparkdf.withColumn('SMID', lit(dataset_ID))
                        sparkdf.write.format("delta").option("mergeSchema", "true").mode("append").save(f"{lh_abfs_path}/Tables/{Table_Name}")

                    

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
