# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
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

def createLakehouse(LH_Name):
    try:
        notebookutils.lakehouse.create(name = LH_Name)
    except Exception as ex:
        print('Lakehouse already exists')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def LoopNotebooks(Notebookname = '', New_LH_name = ''):    
    workspace_id    = fabric.get_workspace_id()    
    items           = fabric.list_items(workspace=workspace_id) 
    currentNotebook = notebookutils.runtime.context['currentNotebookId']

    if(Notebookname != ''):
        itemslist = items[(items['Type'] == 'Notebook') & (items['Id'] != currentNotebook) & (items['Display Name'] == Notebookname)]
    else:
        itemslist = items[(items['Type'] == 'Notebook') & (items['Id'] != currentNotebook)]

    for _, row in itemslist.iterrows():
        notebook_name = row['Display Name']
        remove_all_lakehouses(notebook_name, workspace_id)
    
    if(New_LH_name != ''):
        createLakehouse(LH_Name=New_LH_name)
        add_new_default_notebook(notebook_name, workspace_id, New_LH_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# set variables and load items for this workspace
import sempy.fabric as fabric

# use '' if you want each notebook to get a new lakehouse
notebook_name   = 'Nbt_Landing_GetDataflows'
# use '' if you don't want to add a new lakehouse
LH_name         = 'TestNewLakehouse'


LoopNotebooks(Notebookname = notebook_name, New_LH_name= LH_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# https://fabric.guru/programmatically-removing-updating-default-lakehouse-of-a-fabric-notebook

