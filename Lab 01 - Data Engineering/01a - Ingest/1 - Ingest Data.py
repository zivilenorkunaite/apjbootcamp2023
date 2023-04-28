# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Prepare your lab
# MAGIC
# MAGIC Run the next 2 cells to generate some data we will be using for this lab.
# MAGIC
# MAGIC Data will be stored in a separate location

# COMMAND ----------

# MAGIC %run ../Utils/prepare-lab-environment

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest data from cloud storage
# MAGIC
# MAGIC If your data is already in the cloud - you can simply read it from S3/ADLS 

# COMMAND ----------

products_cloud_storage_location = f'{datasets_location}products/products.json'
df = spark.read.json(products_cloud_storage_location)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Hands On Task!
# MAGIC
# MAGIC Do you remember how to explore this dataset using notebooks?
# MAGIC
# MAGIC Hint: use `display()` or `createOrReplaceTempView()`

# COMMAND ----------

# Explore customers dataset

... # do display(<your dataframe>)

# Create a temporary view (notebook scope - only accessible in this notebok) to query it using SQL
df.createOrReplaceTempView(<your view name>)

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- If you're using SQL you might want to create your 
# MAGIC SELECT * FROM <your view name>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Ingesting new files from same location
# MAGIC
# MAGIC The [`COPY INTO`](https://docs.databricks.com/sql/language-manual/delta-copy-into.html) SQL command lets you load data from a file location into a Delta table. This is a re-triable and idempotent operation; files in the source location that have already been loaded are skipped.
# MAGIC
# MAGIC `FORMAT_OPTIONS ('mergeSchema' = 'true')` - Whether to infer the schema across multiple files and to merge the schema of each file. Default = false. Enabled by default for Auto Loader when inferring the schema.
# MAGIC `COPY_OPTIONS ('mergeSchema' = 'true')` - default false. If set to true, the schema can be evolved according to the incoming data.

# COMMAND ----------

spark.sql("CREATE TABLE IF NOT EXISTS my_products;")

spark.sql(f"""
COPY INTO my_products 
FROM '{datasets_location}products/'
FILEFORMAT = json
FORMAT_OPTIONS ('mergeSchema' = 'true') -- applies schema merge accross all source files
COPY_OPTIONS ('mergeSchema' = 'true') -- applies schema merge on target table if source schema is different
""")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Hands On Task!
# MAGIC

# MAGIC We also have stores dataset available. Write COPY INTO statement for that dataset using `%sql` cell. Note that SQL code example can be copied from the cell above, you will only need to specify correct dataset files location!

# MAGIC
# MAGIC Hint: Use `dbutils.fs.ls(datasets_location)` to find sales dataset files and print that location to get full path for SQL

# COMMAND ----------

dbutils.fs.ls(f"{datasets_location}/stores")

# COMMAND ----------

# MAGIC %sql
# MAGIC

# MAGIC -- You need to first create your table..
# MAGIC CREATE TABLE IF NOT EXISTS my_stores;
# MAGIC
# MAGIC -- You ingest data into your table from datasets_location/stores location
# MAGIC -- Exlore what is your file format.. 
# MAGIC COPY INTO my_stores
# MAGIC FROM 'TODO: <your source file path >'
# MAGIC FILEFORMAT = JSON
# MAGIC FORMAT_OPTIONS ('mergeSchema' = 'true')
# MAGIC COPY_OPTIONS ('mergeSchema' = 'true');


# COMMAND ----------

# MAGIC %md
# MAGIC

# MAGIC ## Hands On Task!

# MAGIC
# MAGIC What would that look using autoloader? You can find syntax for it here: https://docs.databricks.com/getting-started/etl-quick-start.html

# COMMAND ----------

# We need to specify checkpoint location for our autoloader metadata, let's keep it next to the table
checkpoint_path = f"{datasets_location}/checkpoints/stores"

(spark.readStream
  .format("TODO: <autoloader file format>")
  .option("cloudFiles.format", "< TODO: source file format>")
  .option("cloudFiles.schemaLocation", checkpoint_path)
  .load(f"{datasets_location}/stores") #
  .writeStream
  .option("checkpointLocation", checkpoint_path)
  .trigger(availableNow=True)
  .toTable(< TODO: your table name >))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Ingest data from API
# MAGIC
# MAGIC If you want to query data via API you can use a python requests library and https://open-meteo.com/
# MAGIC
# MAGIC
# MAGIC
# MAGIC We will need latitude and longitude for a given location. Look it up on https://www.latlong.net/ or use one of the examples:
# MAGIC   
# MAGIC   Auckland: 
# MAGIC   
# MAGIC     lat: -36.848461
# MAGIC     long: 174.763336
# MAGIC     
# MAGIC     
# MAGIC   Sydney:
# MAGIC   
# MAGIC     lat: -33.868820
# MAGIC     long: 151.209290

# COMMAND ----------

import requests
import json

# replace values with your chosen location
lat = -33.868820
long = 151.209290


today = datetime.datetime.now().strftime("%Y-%m-%d")
start_date =  pd.to_datetime(today) - pd.DateOffset(months=3) + pd.offsets.MonthBegin(-1)
end_date = pd.to_datetime(today)

url = f'https://archive-api.open-meteo.com/v1/era5?latitude={lat}&longitude={long}&start_date={start_date.strftime("%Y-%m-%d")}&end_date={end_date.strftime("%Y-%m-%d")}&hourly=temperature_2m,rain&timezone=auto'

response = requests.get(url)

if response.status_code == 200:
  json_data = sc.parallelize([response.text])
  weather_df = spark.read.json(json_data)
  weather_df.display()

else:
  print('Check your URL for errors!')
  print(response.reason)

# Save weather_df as temp view to be able to access it via %sql
weather_df.createOrReplaceTempView("weather_table")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Hands On Task
# MAGIC
# MAGIC Can you draw a temperature chart using this dataset?
# MAGIC
# MAGIC **Hint**: Maybe switch to SQL and use some of the available [SQL functions](https://docs.databricks.com/sql/language-manual/sql-ref-functions-builtin-alpha.html) 
# MAGIC
# MAGIC
# MAGIC **Hint 2**: Check out how [`arrays_zip()`](https://docs.databricks.com/sql/language-manual/functions/arrays_zip.html) and [`explode()`](https://docs.databricks.com/sql/language-manual/functions/explode.html) work

# COMMAND ----------


# MAGIC %sql
# MAGIC -- Create a temperature over time visualisation
# MAGIC -- HINT - Click on <+> sign next to your result and add visualization tab -> select Line chart and provide time as `x column` and temperature as `y column`
# MAGIC select
# MAGIC   t.*
# MAGIC from
# MAGIC   (
# MAGIC     select
# MAGIC       latitude,
# MAGIC       longitude,
# MAGIC       timezone,
# MAGIC       generationtime_ms,
# MAGIC       explode(
# MAGIC         arrays_zip(<TODO: columns to zip into an array>)
# MAGIC       ) as t
# MAGIC     from
# MAGIC       weather_table
# MAGIC   )


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from weather_ds

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ## Partner Connect
# MAGIC
# MAGIC Partner Connect makes it easy for you to discover data, analytics and AI tools directly within the Databricks platform — and quickly integrate the tools you already use today. 
# MAGIC
# MAGIC With Partner Connect, you can simplify tool integration to just a few clicks and rapidly expand the capabilities of your lakehouse.
