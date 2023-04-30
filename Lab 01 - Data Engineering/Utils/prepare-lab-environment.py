# Databricks notebook source
# MAGIC %pip install holidays==0.14.2 -q

# COMMAND ----------

current_user_id = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
datasets_location = f'/FileStore/tmp/{current_user_id}/datasets/'

dbutils.fs.rm(datasets_location, True)
print(f'Dataset files are generated at location: %s' %datasets_location)

# COMMAND ----------

database_name = current_user_id.split('@')[0].replace('.','_')+'_bootcamp'
spark.sql(f'create database if not exists {database_name};')
spark.sql(f'use {database_name}')
print(f'Created Database: %s' %database_name)

# COMMAND ----------

stores_list = ['AKL01','AKL02','WLG01','SYD01','SYD02','BNE01','BNE02','WLG01','MEL01','MEL02','CBR01','PER02']

country_mapping = {
  'AKL01': 'NZ',
  'AKL02': 'NZ',
  'BNE02': 'AU',
  'CBR01': 'AU',
  'MEL01': 'AU',
  'MEL02': 'AU',
  'PER02': 'AU',
  'SYD01': 'AU',
  'SYD02': 'AU',
  'BNE01': 'AU',
  'WLG01': 'NZ'
  }

# COMMAND ----------

# copy dimensions from git

import os

working_dir = '/'.join(os.getcwd().split('/')[0:5])
git_datasets_location = f'{working_dir}/Datasets/dimensions/'

# move all dimensions to their directories
dimensions  =['products','stores','users']
for dim in dimensions:
  dbutils.fs.cp(f'file:{git_datasets_location}{dim}.json', f'{datasets_location}{dim}/{dim}.json')



# COMMAND ----------

from random import randint, choice
import pandas as pd

from datetime import datetime, timedelta

import datetime
import random
import uuid
import itertools
import holidays

import json
import concurrent.futures


def generate_sale_items():
    # Define the available juice options
    premade_mixes = [
        "ACID Sunshine",
        "Blended Benefits",
        "Bounty Of Benefits",
        "Complete Cleanse",
        "Craze",
        "Drink Your Greens",
        "Drink Your Vitamins",
        "Drinkable Vitamins",
        "Fit Drink",
        "Fit Fuel",
        "Fruit Warehouse",
        "Fruits Of Labor",
        "Get Clean",
        "Healthy",
        "Healthy Hydration",
        "Healthy Resource",
        "Hydration Station",
        "Indulgent",
        "Jeneration",
        "Joyful",
        "Juicy Hydration",
        "Jumble",
        "Jumpstart",
        "Jungle",
        "Just Juicy",
        "Justified",
        "No Excuse",
        "Nothing To Lose",
        "Orange Lake",
        "Packed Punch",
        "Power Punch",
        "Powerful Punch",
        "Pulp Power",
        "Punch",
        "Rapid Reward",
        "Refreshing Reward",
        "SQL Paths",
        "Squeezed Sweetness",
        "Super Squeezed",
        "Tough And Tasty",
        "Worth The Squeeze",
    ]
    fruits = [
        "Apple",
        "Orange",
        "Pineapple",
        "Mango",
        "Peach",
        "Banana",
        "Strawberry",
        "Blueberry",
        "Raspberry",
        "Kiwi",
        "Passionfruit",
    ]

    # Select a random order size
    order_size = random.randint(1, 5)
    regular_cost = {"Small": 5, "Medium": 7, "Large": 9}
    sale_items = []

    for i in range(order_size):
        size = random.choice(["Small", "Medium", "Large"])
        cost = regular_cost[size]
        if random.random() < 0.8:
            notes = ""
        else:
            notes = random.choice(["extra ice", "no ice", "no sugar", "extra sugar"])

        # Determine whether the order is a premade mix or a custom mix
        if random.random() < 0.7:
            # Select a random premade mix
            juice_id = random.choice(premade_mixes)
            sale_items.append(
                {"id": juice_id, "size": size, "notes": notes, "cost": cost}
            )
        else:
            # Select a random combination of fruits
            ingredients = random.choices(fruits, k=random.randint(1, 5))
            juice_id = "custom"
            sale_items.append(
                {
                    "id": juice_id,
                    "size": size,
                    "notes": notes,
                    "cost": cost,
                    "ingredients": ingredients,
                }
            )
    return sale_items


def generate_order(store_id, timestamp, max_loyalty_customer_id=0):
    """Generates a single order"""
    customer_id = 0
    if max_loyalty_customer_id > 0:
        if random.random() < 0.2:
            customer_id = random.randint(0, max_loyalty_customer_id)
  
    sale_record = {
        "id": str(uuid.uuid4()),
        "store_id": store_id,
        "ts": timestamp,
        "state": random.choices(["COMPLETED", "PENDING", "CANCELED"], weights=(70, 10, 20), k=1)[0],
        "payment_method": random.choice(["CASH", "ONLINE", "CARD"]),
        "sale_items": generate_sale_items(),
        "customer_id": customer_id,
        "order_source": random.choice(["ONLINE", "IN-STORE"]),
    }

    # Return the order as a dictionary
    return sale_record

def get_days(start_date, end_date):
    # Initialize empty list to store dates
    dates = []
    # Loop through dates from start_date to end_date and append to list
    while start_date <= end_date:
        dates.append(start_date)
        start_date += timedelta(days=1)
    return dates


def is_busy_time(date, country):
    hour = date.hour
    is_weekend = (
        date.weekday() >= 5
    )  # 5 and 6 represent Saturday and Sunday respectively
    is_summer = (
        date.month >= 12 or date.month <= 2
    )  # assuming summer months are December to February
    is_lunch_hour = 11 <= hour < 13  # assuming lunch hour is from 11am to 1pm
    is_after_work = 16 <= hour < 18  # assuming after work is from 4pm to 6pm
    if (is_weekend or is_summer or is_bank_holiday(date, country)) and (
        10 <= hour <= 14 or 16 <= hour <= 19
    ):
        return True
    elif is_lunch_hour or is_after_work:
        return True
    else:
        return False


def is_bank_holiday(date, country):
    if country == "AU":
        # Get list of Australian bank holidays for the year of the given date
        au_holidays = holidays.AU(years=date.year)
        return date in au_holidays
    elif country == "NZ":
        # Get list of New Zealand bank holidays for the year of the given date
        nz_holidays = holidays.NZ(years=date.year)
        return date in nz_holidays
    else:
        return False


def get_country_code(store_id):
    if store_id in country_mapping.keys():
        country_code = country_mapping[store_id]
    else:
        country_code = "AU"
    return country_code


def store_as_json(df, store_id, day):
    filename = f"{datasets_location}sales/{store_id}-{day}.json".replace(":", "-")
    df.write.mode("Overwrite").json(filename)


def generate_orders_for_single_day(store_id, day):
  max_loyalty_customer_id = 100
  orders = []
  for order_time in get_working_hours(day):
      if is_busy_time(day, get_country_code(store_id)):
        num_orders = randint(4, 20)  # generate up to 20 orders during busy hours
      else:
        num_orders = randint(1, 5)  # generate up to 2 orders during slow hours
      for i in range(num_orders):
        order = generate_order(store_id, order_time, max_loyalty_customer_id)
        orders.append(order)
  return orders


def generate_daily_order_details(store_id, start_date, end_date):
    all_days = get_days(start_date, end_date)
    workers = len(all_days) + 1
    full_list = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        future_to_daily_sales = {
            executor.submit(generate_orders_for_single_day, store_id, day): day for day in all_days
        }
        for future in concurrent.futures.as_completed(future_to_daily_sales):
            day = future_to_daily_sales[future]
            full_list.extend(future.result())
    return full_list

def get_working_hours(day):
  working_hours = []
  for hour in range(7, 21):  # assuming shop is open from 7am to 9pm
    for minute in range(0, 60, 10):  # assuming orders are placed every 10 minutes
      working_hours.append(f"{day.strftime('%Y-%m-%d')} {hour:02d}:{minute:02d}:00")
  return working_hours


def generate_more_orders():
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    hours_orders = []

    order_date = pd.to_datetime(today)
    order_hour = randint(7, 22)

    store_id = random.choice(stores_list)

    # get available customers for the store
    max_loyalty_customer_id = 100

    for minute in range(0, 60, 10):  # assuming orders are placed every 10 minutes
        order_time = f"{order_date.strftime('%Y-%m-%d')} {order_hour:02d}:{minute:02d}:00"
        if is_busy_time(order_date, get_country_code(store_id)):
            num_orders = randint(4, 20)  # generate up to 20 orders during busy hours
        else:
            num_orders = randint(1, 5)  # generate up to 2 orders during slow hours
        for i in range(num_orders):
            timestamp = order_time
            order = generate_order(store_id, timestamp, max_loyalty_customer_id)
            hours_orders.append(order)

    # save full hour of sales to json
    store_as_json(
        spark.createDataFrame(hours_orders).coalesce(1),
        store_id,
        f"{order_date.strftime('%Y-%m-%d')} {order_hour:02d}",
    )
    return "More orders have been generated"

# COMMAND ----------

# rewrite product dataset as CDC feed for inserts

spark.sql(f"""
select *, 'insert' as _change_type, '2023-01-01 00:00:00.000' as _change_timestamp from json.`{datasets_location}products/`
""").write.mode('Overwrite').json(f"{datasets_location}products_cdc/initial-export.json")

          
def generate_product_cdc_data():
  current_timestamp = f'{datetime.datetime.now()}'.replace(':','-')
  spark.sql(f"""
    select 'Punch' as id, 'delete' as _change_type, '2023-03-08 01:05:48.000' as _change_timestamp
  """).write.mode('Overwrite').json(f"{datasets_location}products_cdc/updates-{uuid.uuid4()}-{current_timestamp}.json")
  spark.sql(f"""
    select 'Craze' as id, 'update' as _change_type, 'Extra Blueberry' as name, '2023-03-08 01:05:48.000' as _change_timestamp
     """).write.mode('Overwrite').json(f"{datasets_location}products_cdc/updates-{uuid.uuid4()}-{current_timestamp}.json")
  spark.sql(f"""
    select 'Craze' as id, 'insert' as _change_type, 'DLT' as name, '["Carrot","Beatroot","Ginger"]' as ingredients, '2023-03-08 01:05:48.000' as _change_timestamp
    """).write.mode('Overwrite').json(f"{datasets_location}products_cdc/updates-{uuid.uuid4()}-{current_timestamp}.json")
  
generate_product_cdc_data()

# COMMAND ----------

# Generate sales for the last n months. Stop at midnight day before today
def generate_sales_dataset(n = 3):

  today = datetime.datetime.now().strftime("%Y-%m-%d")
  start_date =  pd.to_datetime(today) - pd.DateOffset(months=n) + pd.offsets.MonthBegin(-1)
  end_date = pd.to_datetime(today)

  with concurrent.futures.ThreadPoolExecutor(max_workers=len(stores_list)) as executor:
        future_to_store_sales = {executor.submit(generate_daily_order_details, store_id, start_date, end_date): store_id for store_id in stores_list}
        for future in concurrent.futures.as_completed(future_to_store_sales):
            store_id = future_to_store_sales[future]
            data = future.result()
            filename = f"{datasets_location}sales/{store_id}-{start_date.strftime('%Y-%m-%d')}-{end_date.strftime('%Y-%m-%d')}.json".replace(':','-')
            dbutils.fs.rm(filename,True)
            dbutils.fs.put(filename,json.dumps(data))
  return "New sales data generated in folder: " + f"{datasets_location}sales/"



# COMMAND ----------

generate_sales_dataset()
