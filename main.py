import json
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_utc_timestamp
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import pyspark.sql.functions as F

os.environ["no_proxy"]="*"
os.chdir('/Users/hugo/airflow/dags/')

# Elasticsearch

es = Elasticsearch([{'host': 'localhost', 'port': 9200, "scheme": "http"}])

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
        df.write.mode("overwrite").parquet("data/prepared/weather")

    # 2nd data source
    def raw_energies():
        print("Getting energies data...")
        # Load data from a source
        data = spark.read.option("header", "true").csv(
            "data/raw/energies.csv", sep=";")

        # Convert 'Date - Heure' column to UTC datetime
        data = data.withColumn(
            "Date - Heure", F.to_utc_timestamp(F.col("Date - Heure"), "Europe/Paris"))

        # Extract 'Date' and 'Heure' columns from 'Date - Heure'
        data = data.withColumn("Date", F.to_date(F.col("Date - Heure")))
        data = data.withColumn("Heure", F.date_format(
            F.col("Date - Heure"), "HH:mm:ss"))

        data.to_csv("data/raw/raw_energies.csv", index=False)

    def prepared_energies():
        print("Preparing energies data...")
        # Load data from a source
        data = spark.read.option("header", "true").csv(
            "data/raw/raw_energies.csv", sep=";")

        # Select relevant columns
        selected_columns = ["Date", "Heure", "Consommation brute gaz (MW PCS 0°C) - GRTgaz",
                            "Consommation brute gaz (MW PCS 0°C) - Teréga",
                            "Consommation brute gaz totale (MW PCS 0°C)",
                            "Consommation brute électricité (MW) - RTE",
                            "Consommation brute totale (MW)"]
        data = data.select(selected_columns)

        # Convert the "Date" column to date format
        data = data.withColumn("Date", F.date_format("Date", "yyyy-MM-dd"))

        # Aggregate data by date and calculate the sum of the consumption columns
        aggregated_data = data.groupBy("Date").agg(
            F.sum("Consommation brute gaz (MW PCS 0°C) - GRTgaz").alias(
                "Consommation brute gaz GRTgaz (MW)"),
            F.sum("Consommation brute gaz (MW PCS 0°C) - Teréga").alias(
                "Consommation brute gaz Teréga (MW)"),
            F.sum("Consommation brute gaz totale (MW PCS 0°C)").alias(
                "Consommation brute gaz totale (MW)"),
            F.sum("Consommation brute électricité (MW) - RTE").alias(
                "Consommation brute électricité RTE (MW)"),
            F.sum("Consommation brute totale (MW)").alias(
                "Consommation brute totale (MW)")
        )

        # Save the aggregated data as Parquet format
        aggregated_data.write.mode("overwrite").parquet("data/prepared/energies")

    # Usage data
    def join():
        print("Joining table on date...")

        df_weather = spark.read.parquet("data/prepared/weather")
        df_energies = spark.read.parquet("data/prepared/energies")

        joined_df = df_weather.join(df_energies, on="date", how="inner")

        joined_df.write.format("parquet").mode("overwrite").save("data/data")

    # Index
    def index():
        print("Indexing...")

        df = spark.read.parquet("data/data")

        # Convert the Spark DataFrame into a list of documents
        documents = df.rdd.map(lambda row: row.asDict()).collect()

        # Indexing data in Elasticsearch using helpers.bulk
        actions = [
            {
                "_index": "data",
                "_source": document
            }
            for document in documents
        ]

        helpers.bulk(es, actions)

        print("test")

        # Check indexation
        res = es.search(index='data', body={"query": {"match_all": {}}})
        print(f"Documents inserted: {res['hits']['total']['value']}")

        # # Kibana dashboard
        # dashboard_config = {
        #     "objects": [
        #         {
        #             "id": "1",
        #             "type": "index-pattern",
        #             "attributes": {
        #                 "title": "data-*",
        #                 "timeFieldName": "timestamp"
        #             }
        #         },
        #         {
        #             "id": "2",
        #             "type": "visualization",
        #             "attributes": {
        #                 "title": "Nom de votre visualisation",
        #                 "visState": "{\"type\":\"visualization type\",\"params\":{\"aggs\":[],\"listeners\":{}},\"title\":\"Nom de votre visualisation\",\"uiStateJSON\":\"{}\"}",
        #                 "uiStateJSON": "{}"
        #             }
        #         },
        #         {
        #             "id": "3",
        #             "type": "dashboard",
        #             "attributes": {
        #                 "title": "Nom de votre tableau de bord",
        #                 "panelsJSON": "[{\"gridData\":{\"x\":0,\"y\":0,\"w\":12,\"h\":6,\"i\":\"2\"},\"panelRefName\":\"panel_0\",\"embeddableConfig\":{\"vis\":{\"id\":\"2\",\"embeddableConfig\":{},\"type\":\"visualization\"}}}]"
        #             },
        #             "references": [
        #                 {"name": "panel_0", "type": "visualization", "id": "2"}
        #             ]
        #         }
        #     ]
        # }
        #
        # # Send the request
        # response = es.transport.perform_request(
        #     method='POST',
        #     url='/_kibana/visualization/_bulk_create',
        #     headers={'Content-Type:application/json'},
        #     body=dashboard_config
        # )

        # Close Spark session
        spark.stop()

    # Operator
    raw_weather = PythonOperator(
        task_id='raw_weather',
        python_callable=raw_weather,
    )

    prepared_weather = PythonOperator(
        task_id='prepared_weather',
        python_callable=prepared_weather,
    )

    raw_energies = PythonOperator(
        task_id='raw_energies',
        python_callable=raw_energies,
    )

    prepared_energies = PythonOperator(
        task_id='prepared_energies',
        python_callable=prepared_energies,
    )

    join = PythonOperator(
        task_id='join',
        python_callable=join,
    )

    index = PythonOperator(
        task_id='index',
        python_callable=index,
    )

    # Tree structure
    raw_weather >> prepared_weather
    raw_energies >> prepared_energies

    prepared_weather >> join
    prepared_energies >> join

    join >> index