# -*- coding: utf-8 -*-
"""
Created on Sun Jun 29 22:55:10 2025

@author: Pongo
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.ml import Pipeline
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, CountVectorizer
from pyspark.ml.classification import NaiveBayes
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# 1) Create Spark session
spark = SparkSession.builder.appName("OpenSentimentTraining").getOrCreate()

# 2) Load the Sentiment140 dataset (already downloaded locally)
raw = (
    spark.read
         .option("header", "false")
         .option("inferSchema", "true")
         .csv(r"C:/Users/Pongo/Downloads/MDIK_Tugas_2/Apache_Spark_Sentiment_Analysis/training.1600000.processed.noemoticon.csv")
)

# 3) Rename and select only (label, text)
data = raw.select(
    (col("_c0") / 4).alias("label"),  # map 0→0.0, 4→1.0
    col("_c5").alias("text")           # review text
)

# 4) Split into train/test
train, test = data.randomSplit([0.8, 0.2], seed=42)

# 5) Build ML pipeline: tokenize → remove stopwords → vectorize → NaiveBayes
tokenizer   = RegexTokenizer(inputCol="text",     outputCol="words",    pattern="\\W+")
stopRemover = StopWordsRemover(inputCol="words",   outputCol="filtered")
vectorizer  = CountVectorizer(inputCol="filtered", outputCol="features", vocabSize=10000)
nb          = NaiveBayes(labelCol="label", featuresCol="features")

pipeline = Pipeline(stages=[tokenizer, stopRemover, vectorizer, nb])

# 6) Train the model
model = pipeline.fit(train)

# 7) Evaluate on held-out test set
predictions = model.transform(test)
evaluator  = MulticlassClassificationEvaluator(
    labelCol="label", predictionCol="prediction", metricName="accuracy"
)
acc = evaluator.evaluate(predictions)
print(f"Test accuracy on Sentiment140 data = {acc:.4f}")

# 8) Read your CSV with the same 'text' column
csv_path = r"C:/Users/Pongo/Downloads/MDIK_Tugas_2/Ingestion_layer/combined_row_by_row.csv"
df = (
    spark.read
         .option("header", "true")
         .option("inferSchema", "true")
         .csv(csv_path)
)

# 9) Drop any existing pipeline columns to avoid conflicts
cols_to_drop = ["words", "filtered", "features", "rawPrediction", "probability", "prediction", "label"]
existing = [c for c in cols_to_drop if c in df.columns]
if existing:
    df = df.drop(*existing)

# 10) Apply the trained pipeline directly to your data
pred = model.transform(df)

# 11) Map numeric predictions → 'good' / 'bad'
result = pred.withColumn(
    "sentiment",
    when(col("prediction") == 1.0, "good").otherwise("bad")
)

# 12) Show sample output
result.select("InvoiceNo", "text", "sentiment").show(10, truncate=80)

# 13) Stop Spark
spark.stop()
