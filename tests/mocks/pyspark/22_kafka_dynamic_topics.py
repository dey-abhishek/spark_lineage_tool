"""Mock PySpark script with dynamically constructed Kafka topics.

Tests the tool's ability to extract and resolve topics constructed using:
- F-strings
- String concatenation
- Variables
- Configuration objects
"""

from pyspark.sql import SparkSession
import sys
import os

# Configuration
ENV = os.getenv("ENVIRONMENT", "prod")
TOPIC_PREFIX = "analytics"
APP_NAME = "user_events"

def main():
    spark = SparkSession.builder \
        .appName("DynamicKafkaTopics") \
        .getOrCreate()
    
    # Example 1: F-string with environment variable
    input_topic_fstring = f"{TOPIC_PREFIX}_{ENV}_raw"
    df_raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", f"topic_{ENV}") \
        .option("startingOffsets", "earliest") \
        .load()
    
    # Example 2: F-string with multiple variables
    output_topic_fstring = f"{TOPIC_PREFIX}_{APP_NAME}_{ENV}"
    df_raw.selectExpr("CAST(value AS STRING)") \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", f"{TOPIC_PREFIX}_processed_{ENV}") \
        .option("checkpointLocation", f"/checkpoints/{ENV}/kafka") \
        .start()
    
    # Example 3: String concatenation
    topic_concat = "events_" + ENV + "_stream"
    df_concat = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "raw_" + ENV) \
        .load()
    
    # Example 4: Variable reference
    configured_topic = os.getenv("KAFKA_TOPIC", "default_topic")
    df_var = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", configured_topic) \
        .load()
    
    # Example 5: Configuration object
    class Config:
        def __init__(self):
            self.env = ENV
            self.input_topic = f"input_{ENV}"
            self.output_topic = f"output_{ENV}"
    
    config = Config()
    df_config = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", config.input_topic) \
        .load()
    
    # Example 6: Command-line argument
    if len(sys.argv) > 1:
        topic_arg = sys.argv[1]
        df_arg = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", topic_arg) \
            .load()
    
    # Example 7: Complex f-string with formatting
    date_partition = "20240115"
    df_complex = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", f"events_{ENV}_{date_partition}") \
        .load()
    
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    # Set environment variables for resolution
    ENV = "prod"
    TOPIC_PREFIX = "analytics"
    APP_NAME = "user_events"
    
    main()

