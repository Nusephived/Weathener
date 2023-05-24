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
        data = spark.read.option("header", "true").csv("data/raw/energies.csv", sep=";")

    def prepared_energies():
        print("Preparing energies data...")
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

        # Check indexation
        res = es.search(index='data', body={"query": {"match_all": {}}})
        print(f"Documents inserted: {res['hits']['total']['value']}")

        # Dashboard config
        dashboard_config = {
            "objects": [
                {
                    "id": "1",
                    "type": "index-pattern",
                    "attributes": {
                        "title": "data-*",
                        "timeFieldName": "timestamp"
                    }
                },
                {
                    "id": "2",
                    "type": "visualization",
                    "attributes": {
                        "title": "Energy Consumption Trend",
                        "visState": "{\"type\":\"line\",\"params\":{\"buckets\":[{\"type\":\"date_histogram\",\"params\":{\"field\":\"Date\",\"interval\":\"day\",\"min_doc_count\":1},\"schema\":\"segment\"}],\"metrics\":[{\"type\":\"sum\",\"params\":{\"field\":\"Consommation brute totale (MW)\"},\"schema\":\"metric\"}],\"listeners\":{}},\"title\":\"Energy Consumption Trend\",\"uiStateJSON\":\"{}\"}",
                        "uiStateJSON": "{}"
                    }
                },
                {
                    "id": "3",
                    "type": "visualization",
                    "attributes": {
                        "title": "Energy Consumption by Source",
                        "visState": "{\"type\":\"table\",\"params\":{\"perPage\":10,\"showMetricsAtAllLevels\":false,\"showPartialRows\":false,\"sort\":{\"columnIndex\":null,\"direction\":null},\"showTotal\":false,\"showToolbar\":true},\"title\":\"Energy Consumption by Source\",\"uiStateJSON\":\"{}\"}",
                        "uiStateJSON": "{}"
                    }
                },
                {
                    "id": "4",
                    "type": "visualization",
                    "attributes": {
                        "title": "Temperature vs. Consumption by Season",
                        "visState": "{\"type\":\"heatmap\",\"params\":{\"categoryAxes\":[{\"id\":\"CategoryAxis-1\",\"type\":\"category\",\"position\":\"left\",\"show\":true,\"style\":{},\"scale\":{\"type\":\"linear\"},\"labels\":{\"show\":true,\"truncate\":100},\"title\":{}},{\"id\":\"CategoryAxis-2\",\"type\":\"category\",\"position\":\"top\",\"show\":true,\"style\":{},\"scale\":{\"type\":\"linear\"},\"labels\":{\"show\":true,\"truncate\":100},\"title\":{}}],\"valueAxes\":[{\"id\":\"ValueAxis-1\",\"name\":\"LeftAxis-1\",\"type\":\"value\",\"position\":\"left\",\"show\":true,\"style\":{},\"scale\":{\"type\":\"linear\",\"mode\":\"normal\",\"distribution\":\"linear\",\"bounds\":\"auto\",\"hardBounds\":false,\"max\":null,\"min\":null},\"labels\":{\"show\":true,\"truncate\":100,\"rotate\":0,\"filter\":false,\"color\":\"black\",\"fontSize\":12,\"fontFamily\":\"Arial\",\"fontWeight\":\"normal\",\"fontStyle\":\"normal\",\"customClass\":\"\",\"formatter\":\"\",\"values\":[],\"scale\":\"linear\"},\"title\":{\"text\":\"\",\"style\":{\"fontSize\":12,\"fontFamily\":\"Arial\",\"fontStyle\":\"normal\",\"fontWeight\":\"bold\",\"color\":\"black\"}}}],\"seriesParams\":[{\"show\":\"true\",\"type\":\"heatmap\",\"mode\":\"normal\",\"data\":{\"id\":\"2\",\"label\":\"Temperature vs. Consumption by Season\",\"mode\":\"normal\",\"color\":{\"label\":\"\",\"max\":\"rgba(233,51,50,1)\",\"min\":\"rgba(82,208,139,1)\",\"gradient\":0,\"bands\":[]}},\"valueAxis\":\"ValueAxis-1\",\"categoryAxis\":\"CategoryAxis-1\",\"drawLinesBetweenPoints\":false,\"showCircles\":true,\"radiusRatio\":9,\"times\":{\"show\":false,\"dateFormat\":\"YYYY-MM-DD HH:mm:ss\",\"value\":false},\"lines\":{\"show\":false,\"mode\":\"slope\",\"fill\":0.5},\"areaFillOpacity\":0.5,\"interpolate\":\"linear\",\"scale\":\"linear\"}],\"addTooltip\":true,\"addLegend\":true,\"legendPosition\":\"right\",\"times\":{\"show\":false,\"dateFormat\":\"YYYY-MM-DD HH:mm:ss\",\"value\":false},\"grid\":{\"categoryLines\":false,\"valueAxis\":\"ValueAxis-1\"},\"thresholdLine\":{\"show\":false,\"thresholdLine\":{\"value\":10,\"width\":1,\"style\":\"full\",\"color\":\"black\",\"label\":\"Threshold\"}},\"labels\":{\"show\":false,\"values\":false,\"last_value\":false,\"valueAxes\":[],\"series\":\"leftAxis-1\"},\"thresholds\":\"\",\"colorSchema\":\"Blues\",\"percentageMode\":false,\"addTimeMarker\":false},\"title\":\"Temperature vs. Consumption by Season\",\"uiStateJSON\":\"{}\"}",
                        "uiStateJSON": "{}"
                    }
                },
                {
                    "id": "5",
                    "type": "dashboard",
                    "attributes": {
                        "title": "Energy and Weather Dashboard NEW",
                        "panelsJSON": "[{\"gridData\":{\"x\":0,\"y\":0,\"w\":6,\"h\":6,\"i\":\"2\"},\"panelRefName\":\"panel_0\",\"embeddableConfig\":{\"vis\":{\"id\":\"2\",\"embeddableConfig\":{},\"type\":\"visualization\"}}},{\"gridData\":{\"x\":6,\"y\":0,\"w\":6,\"h\":6,\"i\":\"3\"},\"panelRefName\":\"panel_1\",\"embeddableConfig\":{\"vis\":{\"id\":\"3\",\"embeddableConfig\":{},\"type\":\"visualization\"}}},{\"gridData\":{\"x\":0,\"y\":6,\"w\":12,\"h\":6,\"i\":\"4\"},\"panelRefName\":\"panel_2\",\"embeddableConfig\":{\"vis\":{\"id\":\"4\",\"embeddableConfig\":{},\"type\":\"visualization\"}}}]"
                    },
                    "references": [
                        {"name": "panel_0", "type": "visualization", "id": "2"},
                        {"name": "panel_1", "type": "visualization", "id": "3"},
                        {"name": "panel_2", "type": "visualization", "id": "4"}
                    ]
                }
            ]
        }

        # Convert dashboard_config to JSON string
        dashboard_config_json = json.dumps(dashboard_config)

        headers = {
            'kbn-xsrf': 'true',
            # Already added when you pass json= but not when you pass data=
            # 'Content-Type': 'application/json',
        }

        response = requests.post('http://localhost:5601/api/saved_objects/_bulk_create', headers=headers,json=dashboard_config_json)

        print(response)
        print(response.text)

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