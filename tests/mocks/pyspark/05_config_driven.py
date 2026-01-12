#!/usr/bin/env python3
"""Config-driven PySpark job."""

from pyspark.sql import SparkSession
import yaml

def load_config(config_file):
    with open(config_file, 'r') as f:
        return yaml.safe_load(f)

config = load_config("config/job.yaml")

spark = SparkSession.builder.appName(config['job_name']).getOrCreate()

# Read from configured sources
for source in config['sources']:
    df = spark.read.format(source['format']).load(source['path'])
    df.createOrReplaceTempView(source['name'])

# Process using configured query
result = spark.sql(config['query'])

# Write to configured outputs
for output in config['outputs']:
    result.write.mode(output['mode']).format(output['format']).save(output['path'])

spark.stop()

