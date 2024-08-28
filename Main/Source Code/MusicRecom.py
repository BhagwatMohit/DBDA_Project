#!/usr/bin/env python3
# coding: utf-8

from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.sql.functions import col, abs
from pyspark.sql.functions import col, format_number
from pyspark.sql.functions import regexp_replace
import sys
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

spark = SparkSession.builder \
        .appName("SongRecommendation") \
        .getOrCreate()



# Define the schema for the JSON data
schema = StructType([
    StructField("_id", StructType([
        StructField("$oid", StringType(), True)
    ])),
    StructField("SongName", StringType(), True),
    StructField("ArtistName", StringType(), True),
    StructField("Popularity", IntegerType(), True),
    StructField("Danceability", DoubleType(), True),
    StructField("Energy", DoubleType(), True),
    StructField("Key", IntegerType(), True),
    StructField("Loudness", DoubleType(), True),
    StructField("Mode", IntegerType(), True),
    StructField("Speechiness", DoubleType(), True),
    StructField("Acousticness", DoubleType(), True),
    StructField("Instrumentalness", DoubleType(), True),
    StructField("Liveness", DoubleType(), True),
    StructField("Valence", DoubleType(), True),
    StructField("Tempo", DoubleType(), True),
    StructField("Duration_ms", IntegerType(), True)
])

# Read the text file and split the lines
#lines = spark.read.json(sys.stdin).rdd.map(lambda row: row.value)

# Parse JSON strings into DataFrame
#df = spark.read.json(lines, schema=schema)
lines = spark.read.text(sys.stdin.read()).rdd.map(lambda row: row.value)

# Parse JSON strings into DataFrame
try:
    df = spark.read.json(lines, schema=schema)
    df.show()
except Exception as e:
    print("Error:", e)

#def load_data(sys.stdin):
    # Load data from Kafka consumer
    #df = spark \
    #    .read \
    #    .format("kafka") \
    #    .option("kafka.bootstrap.servers", "localhost:9092") \
    #    .option("subscribe", "MusicM") \
    #    .option("startingOffsets", "earliest") \
    #    .load()

    # Assuming the data is in JSON format, deserialize it
#    df = df.selectExpr("CAST(value AS STRING)")
#    df = df.selectExpr("CAST(value AS JSON)").select("value.*")
#    df = df.dropDuplicates()

#    df = df.withColumn("Loudness", abs(col("Loudness")))


 #   df = df.withColumn("Instrumentalness", format_number(col("Instrumentalness"), 10))


#    df = df.withColumn("Instrumentalness",regexp_replace(col("Instrumentalness"), "(\\.[0-9]*?)0+$", "$1"))

#    df = df.withColumn("Instrumentalness", col("Instrumentalness").cast("int"))
#    df = df.withColumn("Instrumentalness", regexp_replace(col("Instrumentalness"), "[^0-9]", ""))
#    df = df.withColumn("Instrumentalness", col("Instrumentalness").cast("int"))
#    columns_to_drop = ['features']
#    df = df.drop(*columns_to_drop)

#    return df

df = df.withColumn("Loudness", abs(col("Loudness")))
df = df.withColumn("Instrumentalness", format_number(col("Instrumentalness"), 10))
df = df.withColumn("Instrumentalness",regexp_replace(col("Instrumentalness"), "(\\.[0-9]*?)0+$", "$1"))
df = df.withColumn("Instrumentalness", col("Instrumentalness").cast("int"))
df = df.withColumn("Instrumentalness", regexp_replace(col("Instrumentalness"), "[^0-9]", ""))
df = df.withColumn("Instrumentalness", col("Instrumentalness").cast("int"))
columns_to_drop = ['features']
df = df.drop(*columns_to_drop)
usr_input = input("Enter your Artist/Song-name: ")


def train_kmeans_model(df, usr_input):
    # Assemble features
        # Step 1: Assemble features into a single vector column
    feature_cols = ['Popularity', 'Danceability', 'Energy', 'Key', 'Loudness', 'Mode', 'Speechiness', 
                    'Acousticness', 'Instrumentalness', 'Liveness', 'Valence', 'Tempo', 'Duration_ms']
    assembler = VectorAssembler(inputCols=feature_cols, outputCol='features')
    df_assembled = assembler.transform(df)

    # Step 2: Scale the features to standardize them
    scaler = StandardScaler(inputCol='features', outputCol='scaled_features', withMean=True, withStd=True)
    scaler_model = scaler.fit(df_assembled)
    df_scaled = scaler_model.transform(df_assembled)
    
    kmeans = KMeans(featuresCol = "features", k = 40)


    model = kmeans.fit(df_scaled)

    predictions = model.transform(df_scaled)
        
    def get_cluster_number(dataframe, search_term):
        # Search for the search term in either 'song name' or 'Artist name' column
        filtered_df = dataframe.filter((col("SongName") == search_term) | (col("ArtistName") == search_term))
        
        # If no matching record found, return None
        if filtered_df.count() == 0:
            return None
        
        # Get the cluster number from the first matching record
        cluster_number = filtered_df.select("prediction").first()[0]
        return cluster_number
    search_string = usr_input  # Replace with the search string you want to use
    cluster_number = get_cluster_number(predictions, search_string)
    filtered_df = predictions.filter(predictions['prediction'] == cluster_number)
    json_array = filtered_df.toJSON().collect()
    # Print the JSON array 
    print("JSON array:",json_array)
    
    return 

model = train_kmeans_model(df, usr_input)

