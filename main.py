from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import os
import pandas as pd
from data_consomation import convertir_consommation
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_utc_timestamp

os.environ["no_proxy"]="*"
os.chdir('/Users/hugo/airflow/dags/')

# Spark

spark = SparkSession.builder.appName("Weathener").getOrCreate()

# Airflow

with DAG(
        'Weathener',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='ISEP project',
        schedule_interval=None,
        start_date=datetime(2022, 1, 1),
        catchup=False,
        tags=['Weather', 'Energies'],
) as dag:
    dag.doc_md = """Link between weather and energies consumption in Paris, France."""

    # 1st data source
    def raw_weather():
        print("Getting weather data...")

        headers = {
            'x-rapidapi-host': 'meteostat.p.rapidapi.com',
            'x-rapidapi-key': '49fb02cec0mshcda3381b4334509p148a16jsn66939543528d',
        }

        params = {
            'station': '07156',
            'start': '2022-01-01',
            'end': '2022-12-31',
        }

        response = requests.get('https://meteostat.p.rapidapi.com/stations/daily', params=params, headers=headers)
        response = response.json()
        df = pd.DataFrame(response["data"])
        df.to_csv("data/raw/weather.csv", index=False)

    def prepared_weather():
        print("Preparing weather data...")

        df = spark.read.format('csv').options(header='true', delimiter=',').load('data/raw/weather.csv')

        columns_to_drop = ["snow", "wpgt", "tsun", "wdir", "wspd", "wpgt", "pres"]
        df = df.drop(*columns_to_drop)
        df = df.withColumn("date", to_utc_timestamp("date", "UTC"))

        df.show()
        df.write.csv("data/prepared/weather", header=True)

    # 2nd data source
    def raw_energies():
        print("Hello Airflow - This is Task 2")

    def prepared_energies():
        convertir_consommation()
        print("Hello Airflow - This is Prepared 2")

    # Usage data
    def join():
        print("Joining table on date...")

        # Load the Parquet datasets into Spark DataFrames
        df1 = spark.read.parquet("dataset1.parquet")
        df2 = spark.read.parquet("dataset2.parquet")

        # Perform the join operation
        joined_df = df1.join(df2, on="date", how="inner")

        # Show the result or save it to a file
        joined_df.write.format("parquet").mode("overwrite").save("data/data.parquet")

    # Index
    def index():
        print("Hello Airflow - This is Index")

    # Operator
    raw_1 = PythonOperator(
        task_id='raw_weather',
        python_callable=raw_weather,
    )

    prepared_1 = PythonOperator(
        task_id='prepared_weather',
        python_callable=prepared_weather,
    )

    raw_2 = PythonOperator(
        task_id='raw_energies',
        python_callable=raw_energies,
    )

    prepared_2 = PythonOperator(
        task_id='prepared_energies',
        python_callable=prepared_energies,
    )

    data = PythonOperator(
        task_id='data',
        python_callable=join,
    )

    index = PythonOperator(
        task_id='index',
        python_callable=index,
    )

    # Tree structure
    raw_1 >> prepared_1
    raw_2 >> prepared_2

    prepared_1 >> data
    prepared_2 >> data

    data >> index