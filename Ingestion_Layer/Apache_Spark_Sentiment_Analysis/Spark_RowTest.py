# -*- coding: utf-8 -*-
"""
Created on Sun Jun 29 22:22:05 2025

@author: Pongo
"""

from pyspark.sql import SparkSession

# 1. Create a SparkSession (only needs to happen once per application)
spark = SparkSession.builder \
    .appName("ViewCSV10Rows") \
    .getOrCreate()

# 2. Read your CSV file
#    - header=True treats the first line as column names
#    - inferSchema=True asks Spark to guess data types
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(r"C:/Users/Pongo/Downloads/MDIK_Tugas_2/Ingestion_layer/combined_row_by_row.csv")

# 3. Show the first 10 rows in a nicely formatted table
df.show(10)

# 4. (Optional) Stop the SparkSession when you're done
spark.stop()
