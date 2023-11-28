import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, FloatType

# Create Spark session
spark = SparkSession.builder\
    .master("local[*]")\
    .appName("CustomerMonthlyCharges")\
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")\
    .getOrCreate()

# Create Kafka DStream
kafka_df = spark.readStream.format("kafka")\
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("subscribe", "customer-info-topic")\
    .option("startingOffsets", "latest")\
    .load()

# Define the schema for the JSON data
json_schema = StructType([
    StructField("CustomerID", StringType(), True),
    StructField("Age", IntegerType(), True),
    StructField("Gender", StringType(), True),
    StructField("Location", StringType(), True),
    StructField("ServiceUsage", StructType([
        StructField("Duration", IntegerType(), True),
        StructField("NumCalls", IntegerType(), True),
        StructField("NumMessages", IntegerType(), True),
        StructField("DataUsage", FloatType(), True)
    ]), True),
    StructField("BillingInfo", StructType([
        StructField("MonthlyCharges", FloatType(), True),
        StructField("PaymentMethod", StringType(), True)
    ]), True),
    StructField("ChurnStatus", StructType([
        StructField("Churned", BooleanType(), True),
        StructField("ChurnDate", StringType(), True)
    ]), True)
])

# Parse JSON data to DataFrame
parsed_df = kafka_df.selectExpr("CAST(value AS STRING)")\
    .select(from_json("value", json_schema).alias("data"))\
    .select("data.*")

# Display the DataFrame
parsed_df.printSchema()

#هنكمل

