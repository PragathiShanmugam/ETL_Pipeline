
import os
from pyspark.sql import SparkSession
import boto3
import pandas as pd

spark = SparkSession.builder            .appName('Python Spark')            .config("spark.sql.execution.arrow.enabled", "true")            .getOrCreate()
