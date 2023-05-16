from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import pandas as pd

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
        description='Weathener project',
        schedule_interval=None,
        start_date=datetime(2022, 1, 1),
        catchup=False,
        tags=['Weather', 'Energies'],
) as dag:
    dag.doc_md = """Link between weather and energies consumption in Paris, France."""

    # 1st data source
    def raw_1():
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

    def prepared_1():
        print("Preparing weather data...")

    # 2nd data source
    def raw_2():
        print("Hello Airflow - This is Task 2")

    def prepared_2():
        print("Hello Airflow - This is Prepared 2")

    # Usage data
    def data():
        print("Hello Airflow - This is Data")

    # Index
    def index():
        print("Hello Airflow - This is Index")

    # Operator
    raw_1 = PythonOperator(
        task_id='raw_1',
        python_callable=raw_1,
    )

    prepared_1 = PythonOperator(
       task_id='prepared_1',
       python_callable=prepared_1,
    )

    raw_2 = PythonOperator(
       task_id='raw_2',
       python_callable=raw_2
   )

    prepared_2 = PythonOperator(
       task_id='prepared_2',
       python_callable=prepared_2,
   )

    data = PythonOperator(
        task_id='data',
        python_callable=data,
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