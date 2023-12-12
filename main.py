import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when, count, avg
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, FloatType
from mysql_handler import MySQLHandler
import mysql.connector


# Create Spark session
spark = SparkSession.builder\
    .appName("CustomerChurnAnalysis")\
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4")\
    .config("spark.jars", "C:\spark\jars\mysql-connector-j-8.2.0.jar") \
    .getOrCreate()

    # .config("spark.driver.extraClassPath", "D:\DATA\spark-3.2.4\jars\mysql-connector-j-8.2.0.jar") \
    # .config("spark.executor.extraClassPath", "D:\DATA\spark-3.2.4\jars\mysql-connector-j-8.2.0.jar") \

# Create Kafka DStream
kafka_df = spark.readStream.format("kafka")\
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("subscribe", "customer-info-topic")\
    .option("startingOffsets", "earliest")\
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

# Analysis based on demographic factors
demographic_analysis = parsed_df.groupBy("Age", "Gender", "Location").agg(
    count(when(col("ChurnStatus.Churned") == True, 1)).alias("ChurnedCustomers"),
    count(when(col("ChurnStatus.Churned") == False, 1)).alias("NotChurnedCustomers")
)
# Save demographic analysis to MySQLc
demographic_analysis.writeStream \
    .outputMode("complete") \
    .foreachBatch(lambda batch_df, batch_id: MySQLHandler.save_to_mysql(batch_df, "demographic_analysis")) \
    .start()

# Analysis based on behavior patterns
behavior_analysis = parsed_df.groupBy("CustomerID").agg(
    avg(when(col("ChurnStatus.Churned") == True, 1)).alias("AvgChurnFlag")
)

# Save behavior analysis to MySQL
behavior_analysis.writeStream \
    .outputMode("complete") \
    .foreachBatch(lambda batch_df, batch_id: MySQLHandler.save_to_mysql(batch_df, "behavior_analysis")) \
    .start()

# Analysis based on interactions with the company
interaction_analysis = parsed_df.groupBy("CustomerID").agg(
    count(when(col("ServiceUsage.NumCalls") > 0, 1)).alias("NumServiceCalls")
)

# Save interaction analysis to MySQL
interaction_analysis.writeStream \
    .outputMode("complete") \
    .foreachBatch(lambda batch_df, batch_id: MySQLHandler.save_to_mysql(batch_df, "interaction_analysis")) \
    .start()

# Wait for the termination of the queries
spark.streams.awaitAnyTermination()


