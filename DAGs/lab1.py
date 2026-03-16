from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import timedelta
from datetime import datetime
import snowflake.connector
import requests

default_args = {
    "owner": "lab_group_12",
    "email": ["eric.e.zhao@sjsu.edu"],
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "depends_on_past": False,
    "start_date": datetime(2026, 3, 3),
    "catchup": False,
}

def return_snowflake_conn():

    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conid')

    # Execute the query and fetch results
    conn = hook.get_conn()
    return conn.cursor()

@task
def extract(latitude,longitude):
    """Get the past 60 days of weather for a given pair of coordinates"""

    url = "https://api.open-meteo.com/v1/forecast"

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "past_days": 60,
        "forecast_days": 0,  # only past weather
        "daily": [
            "temperature_2m_max",
            "temperature_2m_min",
            "weather_code"
        ],
        "timezone": "America/Los_Angeles"
    }

    response = requests.get(url, params=params)

    if response.status_code != 200:
        raise RuntimeError(f"API request failed: {response.status_code}")

    return response.json()

@task
def transform(raw_data, latitude, longitude, city):

    if "daily" not in raw_data:
        raise ValueError("Missing 'daily' key in API response")

    data = raw_data["daily"]

    # instead of using datafram, just create a dictionary here
    records = []
    for i in range(len(data["time"])):
        
        tmax = data["temperature_2m_max"][i]
        tmin = data["temperature_2m_min"][i]
        
        if tmax is None or tmin is None:
            tmean = None
        else:
            tmean = (tmax + tmin) / 2
            
        records.append({
          "location_name": city,
          "latitude": latitude,
          "longitude": longitude,
          "date": data["time"][i],
          "temp_max": data["temperature_2m_max"][i],
          "temp_min": data["temperature_2m_min"][i],
          "temp_mean": tmean,
          "weather_code": data["weather_code"][i]
          })
    return records

@task
def load(con, target_table,records):
  try:
    con.execute("BEGIN;")
    con.execute(f"""CREATE TABLE IF NOT EXISTS {target_table} (
        location_name VARCHAR(255),
        latitude FLOAT,
        longitude FLOAT,
        date DATE,
        temp_max FLOAT,
        temp_min FLOAT,
        temp_mean FLOAT,
        weather_code VARCHAR(3),
        PRIMARY KEY (latitude, longitude, date, location_name));""")

    con.execute(f"""DELETE FROM {target_table};""")

    insert_sql = f"""INSERT INTO {target_table} (
        location_name,
        latitude,
        longitude,
        date,
        temp_max,
        temp_min,
        temp_mean,
        weather_code
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);"""

    data = [
        (
            r["location_name"],
            r["latitude"],
            r["longitude"],
            r["date"],
            r["temp_max"],
            r["temp_min"],
            r["temp_mean"],
            r["weather_code"]
        )
        for r in records
    ]

    con.executemany(insert_sql, data)

    con.execute("COMMIT;")
    print(f"Loaded {len(records)} records into {target_table}.")

  except Exception as e:
    con.execute("ROLLBACK;")
    print(e)
    raise e

@task
def combine(a, b):
    """Combine two lists of records into one list"""
    return (a or []) + (b or [])

with DAG(
    dag_id="lab1_weatherdata_etl",
    start_date = datetime(2026, 3, 3),
    catchup=False,
    tags = ["ETL"],
    default_args=default_args,
    schedule = "30 2 * * *"
) as dag:
    SF_LATITUDE = Variable.get("SF_LATITUDE")
    SF_LONGITUDE = Variable.get("SF_LONGITUDE")
    SF = "San Francisco"

    SJ_LATITUDE = Variable.get("SJ_LATITUDE")
    SJ_LONGITUDE = Variable.get("SJ_LONGITUDE")
    SJ = "San Jose"

    target_table = "raw.weather_data_lab1"
    cur = return_snowflake_conn()

    # Extract twice

    sf_raw_data = extract(SF_LATITUDE, SF_LONGITUDE)
    sj_raw_data = extract(SJ_LATITUDE, SJ_LONGITUDE)

    # Transform twice

    sf_records = transform(sf_raw_data, SF_LATITUDE, SF_LONGITUDE, SF)
    sj_records = transform(sj_raw_data, SJ_LATITUDE, SJ_LONGITUDE, SJ)

    all_records = combine(sf_records, sj_records)

    load(cur, target_table, all_records)