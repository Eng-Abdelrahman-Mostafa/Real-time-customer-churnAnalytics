from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType
from data_schema import DataSchema

# Set up SparkSession
spark = SparkSession.builder.appName("ChurnAnalysis").getOrCreate()

# Define the schema for the Kafka stream
data_schema = DataSchema.schema()

kafka_schema = StructType.fromJson(data_schema)

# Read data from Kafka topics
stream_data = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "customer-info-topic").load()

# Deserialize JSON data from Kafka
parsed_data = stream_data.select(from_json(col("value").cast("string"), kafka_schema).alias("data")).select("data.*")

# Perform basic analytics (count by gender as an example)
gender_counts = parsed_data.groupBy("Gender").count()

# Write the results to the console
query = gender_counts.writeStream.outputMode("complete").format("console").start()

# Wait for the termination of the query
query.awaitTermination()


