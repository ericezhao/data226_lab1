from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task

from datetime import timedelta
from datetime import datetime
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import requests


def return_snowflake_conn():

    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conid')
    
    # Execute the query and fetch results
    conn = hook.get_conn()
    return conn.cursor()


@task
def train(cur, train_input_table, train_view, forecast_function_name):
    """
     - Create a view with training related columns
     - Create a model with the view above
    """
    # Create a view selecting specific columns required for time-series forecasting.
    # Specifically uses DATE, TEMP_MAX, and LOCATION_NAME from the source weather table.
    create_view_sql = f"""CREATE OR REPLACE VIEW {train_view} AS 
    (SELECT
        DATE AS ds, 
        TEMP_MAX,
        LOCATION_NAME AS series
        FROM {train_input_table});"""

    # Define and train the ML model targeting Maximum Temperature.
    # The view aliases LOCATION_NAME as 'series'; the model must use that column name.
    create_model_sql = f"""CREATE OR REPLACE SNOWFLAKE.ML.FORECAST {forecast_function_name} (
        INPUT_DATA => SYSTEM$REFERENCE('VIEW', '{train_view}'),
        SERIES_COLNAME => 'series',
        TIMESTAMP_COLNAME => 'ds',
        TARGET_COLNAME => 'TEMP_MAX',
        CONFIG_OBJECT => {{ 'ON_ERROR': 'SKIP' }}
    );"""

    try:
        cur.execute(create_view_sql)
        cur.execute(create_model_sql)
        # Inspect the accuracy metrics of your model. 
        cur.execute(f"CALL {forecast_function_name}!SHOW_EVALUATION_METRICS();")
    except Exception as e:
        print(e)
        raise


@task
def predict(cur, forecast_function_name, train_input_table, forecast_table, final_table):
    """
     - Generate predictions and store the results to a table named forecast_table.
     - Union your predictions with your historical data, then create the final table
    """
    make_prediction_sql = f"""BEGIN
        CALL {forecast_function_name}!FORECAST(
            FORECASTING_PERIODS => 7,
            -- Here we set your prediction interval.
            CONFIG_OBJECT => {{'prediction_interval': 0.95}}
        );
        -- These steps store your predictions to a table.
        LET x := SQLID;
        CREATE OR REPLACE TABLE {forecast_table} AS SELECT * FROM TABLE(RESULT_SCAN(:x));
    END;"""
    create_final_table_sql = f"""CREATE OR REPLACE TABLE {final_table} AS
        SELECT * FROM (
            SELECT LOCATION_NAME, DATE, TEMP_MAX as actual, NULL AS forecast, NULL AS lower_bound, NULL AS upper_bound
            FROM {train_input_table}
            UNION ALL
            SELECT replace(series, '"', '') as LOCATION_NAME, ts as DATE, NULL AS actual, forecast, lower_bound, upper_bound
            FROM {forecast_table}
        )
        ORDER BY LOCATION_NAME, DATE;"""

    try:
        cur.execute(make_prediction_sql)
        cur.execute(create_final_table_sql)
    except Exception as e:
        print(e)
        raise


with DAG(
    dag_id = 'TrainPredict_lab1',
    start_date = datetime(2026,3,3),
    catchup=False,
    tags=['ML', 'ELT'],
    schedule = '30 2 * * *'
) as dag:

    train_input_table = "raw.weather_data_lab1"
    train_view = "adhoc.city_weather_view"
    forecast_table = "adhoc.city_weather_forecast"
    forecast_function_name = "analytics.predict_city_weather"
    final_table = "analytics.city_weather_final_lab1"
    
    cur = return_snowflake_conn()
    cur.execute("CREATE SCHEMA IF NOT EXISTS adhoc")
    cur.execute("CREATE SCHEMA IF NOT EXISTS analytics")

    train_task = train(cur, train_input_table, train_view, forecast_function_name)
    predict_task = predict(cur, forecast_function_name, train_input_table, forecast_table, final_table)
    train_task >> predict_task