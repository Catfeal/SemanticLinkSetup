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

# De code is zodanig geschreven dat je van bovenaf moet beginnen om alle definities, benamingen,... mee te hebben in de latere code.
# Maar ook dat er maar 1 plaats is waar je tabelnamen moet aanpassen.
# 
# de parameters dienen om de code sneller te laten lopen als je gewoon wilt testen omdat je dan ook nog steeds de hele code moet laten lopen.
# Je kan dit in principe vermijden door elke keer uit de SQL tabellen te gaan lezen, maar dat maakt de code ook enorm, enorm traag

# CELL ********************

%run Fnc_StartupFabricSempy

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

prm_starttime = datetime.now()
NotebookName = "Nbt_Landing_GetItems"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

Table_Name = 'Landing_Fabric_Items'

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

#fill list of workspaces
#workspaces = fabric.list_workspaces().sort_values(by='Name', ascending=True)
#df = pd.DataFrame(workspaces)

Workspaces = spark.sql("""select Id, Name
from Lkh_SemanticLink.Landing_Fabric_Workspaces""")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for Id, Name in Workspaces.toLocalIterator():
    temp_items = fabric.list_items(workspace=Id)
    itemdf = pd.DataFrame(temp_items)
    if not itemdf.empty: # check if the list is not empty to avoid errors
        #prepare items and write them away
        try:
            itemdf = fnc_PrepareColumns(itemdf)
            sparkdf = spark.createDataFrame(itemdf)
            sparkdf = sparkdf.withColumn('WSID', lit(Id))
            sparkdf.write.format("delta").option("mergeSchema", "true").mode("append").save(f"{lh_abfs_path}/Tables/{Table_Name}")
            #print(Table_Name_Items, "created at :", f"{lh_abfs_path}/Tables/{Table_Name_Items}")                        
        except Exception as e:
            print(f"Error fetching Workspace objects for {Name}: {e}")
            continue

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# loop through the workspaces
for label, content in df.items():
    if label == 'Id': #loop through the ID's
        for id in content:
            #lijst vullen met alle items in de werkruimte
            temp_items = fabric.list_items(workspace=id)
            itemdf = pd.DataFrame(temp_items)
            if not itemdf.empty: # check if the list is not empty to avoid errors
                #prepare items and write them away
                try:
                    itemdf = fnc_PrepareColumns(itemdf)
                    sparkdf = spark.createDataFrame(itemdf)
                    sparkdf = sparkdf.withColumn('WSID', lit(id))
                    sparkdf.write.format("delta").option("mergeSchema", "true").mode("append").save(f"{lh_abfs_path}/Tables/{Table_Name}")
                    #print(Table_Name_Items, "created at :", f"{lh_abfs_path}/Tables/{Table_Name_Items}")                        
                except Exception as e:
                    print(f"Error fetching Workspace objects for {id}: {e}")
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
