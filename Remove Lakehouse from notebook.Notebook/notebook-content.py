# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# get items in current workspace

# CELL ********************

import sempy.fabric as fabric
notebook_name = 'Nbt_Landing_GetDataflows'
workspace_id = fabric.get_workspace_id()
items = fabric.list_items(workspace=workspace_id)
print(items)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

currentNotebook = notebookutils.runtime.context['currentNotebookId']
itemslist = items[(items['Type'] == 'Notebook') & (items['Id'] != currentNotebook)]
for _, row in itemslist.iterrows():
    notebookID = row['Id']
    notebook_name = row['Display Name']
    if(notebook_name == 'Nbt_Landing_GetDataflows'):
        print(notebookID, notebook_name)
        remove_all_lakehouses(notebook_name, workspace_id)
        add_new_default_notebook(notebook_name, workspace_id, "Lkh_Testing_FromDifferenetNotebook")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json

def remove_all_lakehouses(notebook_name, workspace_id):
    try:
        nb = json.loads(notebookutils.notebook.getDefinition(notebook_name, workspaceId=workspace_id))
    except:
        print("Error, check notebook & workspace id")

    if 'dependencies' in nb['metadata'] and 'lakehouse' in nb['metadata']['dependencies']:
        # Remove all lakehouses
        nb['metadata']['dependencies']['lakehouse'] = {}
    # Update the notebook definition without any lakehouses
    notebookutils.notebook.updateDefinition(
        name=notebook_name,
        content=json.dumps(nb),
        workspaceId=workspace_id
    )

    print(f"All lakehouses have been removed from notebook '{notebook_name}'.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def add_new_default_notebook(notebook_name, workspaceid, defaultlakehouse):
    notebookutils.notebook.updateDefinition(
        name = notebook_name, 
        workspaceId=workspaceid,
        defaultLakehouse=defaultlakehouse, 
        defaultLakehouseWorkspace=workspaceid
        )




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

remove_all_lakehouses(notebook_name, workspace_id)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# https://fabric.guru/programmatically-removing-updating-default-lakehouse-of-a-fabric-notebook

