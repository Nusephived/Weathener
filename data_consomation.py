import pyspark.sql.functions as F
from pyspark.sql import SparkSession

def convertir_consommation():
    # Create a Spark session
    spark = SparkSession.builder.appName("Consommation").getOrCreate()

    # Load data from a source
    data = spark.read.option("header", "true").csv(
        "data/raw/energies.csv", sep=";")

    # Select relevant columns
    selected_columns = ["Date", "Consommation brute totale (MW)"]
    data = data.select(selected_columns)

    # Convert the "Date - Heure" column to date format
    data = data.withColumn("Date", F.date_format("Date", "yyyy-MM-dd"))

    # Aggregate data by date and calculate the sum of the total raw consumption
    aggregated_data = data.groupBy("Date").agg(
        F.sum("Consommation brute totale (MW)").alias("Consommation totale (MW)"))

    # Show the results
    aggregated_data.show()