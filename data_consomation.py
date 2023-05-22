import pyspark.sql.functions as F
from pyspark.sql import SparkSession
#dépendances nécessaires pyspark et pyarrow


def convertir_consommation():
    # Create a Spark session
    spark = SparkSession.builder.appName("Consommation").getOrCreate()

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

    # Show the results
    aggregated_data.show()

    # Save the aggregated data as Parquet format
    aggregated_data.write.mode("overwrite").parquet(
        "data/prepared/energies")
