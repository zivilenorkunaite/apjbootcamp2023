# Databricks notebook source
import json
import requests

from databricks_cli.sdk import ApiClient
from databricks_cli.pipelines.api import PipelinesApi
from databricks_cli.jobs.api import JobsApi

current_user_id = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()

_notebook_ctx = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())

api_host = 'https://' + _notebook_ctx['tags']['browserHostName']
api_token = _notebook_ctx['extraContext']['api_token']

# Provide a host and token
db = ApiClient(
    host=api_host,
    token=api_token
)

def get_warehouse_ids():
  warehouse_uuid = ''
  warehouse_id = ''

  all_warehouses = requests.get(
    f'{api_host}/api/2.0/sql/warehouses',
    headers={'Authorization': f'Bearer {api_token}'},
  )

  if all_warehouses.status_code == 200:
    find_serverless_and_pro = [i for i in all_warehouses.json()['warehouses'] if (i['warehouse_type'] == 'PRO' ) and i['state'] == 'RUNNING' ]
    if len(find_serverless_and_pro) == 0:
      find_serverless_and_pro = [i for i in all_warehouses.json()['warehouses'] if (i['warehouse_type'] == 'PRO' )]
    if len(find_serverless_and_pro) == 0:
      print("No warehouses found")
    else:
      warehouse_id = find_serverless_and_pro[0]['id']
    
    # get data source id for this warehouse
    all_data_sources = requests.get(
      f'{api_host}/api/2.0/preview/sql/data_sources',
      headers={'Authorization': f'Bearer {api_token}'},
    )

    if all_data_sources.status_code == 200:
      find_uuid =  [i for i in all_data_sources.json() if (i['warehouse_id'] == warehouse_id ) ]
      warehouse_uuid = find_uuid[0]['id']
    else:
      print("Error getting data sources: %s" % (all_data_sources.json()))
  else:
    print("Error getting warehouses: %s" % (all_warehouses.json()))
  
  return warehouse_uuid, warehouse_id
  
def create_new_sql_query(warehouse_id, sql_query_name, sql_text):
  
  query_id = ''

  if warehouse_id != '':
    new_query = requests.post(
        f'{api_host}/api/2.0/preview/sql/queries',
        headers={'Authorization': f'Bearer {api_token}'},
      json={
        "data_source_id": warehouse_id,
        "description": "Demo SQL query",
        "name": sql_query_name,
        "options": {
          "parameters": [
            {
              "name": "count_threshold",
              "title": "Count Threshold",
              "type": "number",
              "value": "5"
            }
          ]
      },
        "query": sql_text,
      }
    )

    if new_query.status_code == 200:
      print(new_query.json()['id'])
    else:
      print("Error creating query: %s" % (new_query.json()))
      
    query_id = new_query.json()['id']
    return query_id




# COMMAND ----------

database_name = dbutils.notebook.run(path="../Lab 02 - SQL/01-SetUp-Env", timeout_seconds=120)
warehouse_uuid, warehouse_id = get_warehouse_ids()

# COMMAND ----------

sql_query_name = "APJ-Bootcamp-Demo-SQL1"

sql_query = f"""SELECT
  a.store_id,
  a.order_source,
  a.order_state,
  b.city,
  b.country_code,
  b.name AS store_name,
  count(*) AS cnt
FROM
  {database_name}.fact_apj_sales a
  JOIN {database_name}.dim_store_locations b ON a.slocation_skey = b.slocation_skey
GROUP BY
  a.store_id,
  a.order_source,
  a.order_state,
  b.city,
  b.country_code,
  b.name
HAVING cnt > {{{{count_threshold}}}}"""


query_id = create_new_sql_query(warehouse_uuid, sql_query_name, sql_query)

