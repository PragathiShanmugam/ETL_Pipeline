
import os
from pyspark.sql import SparkSession
import boto3
import pandas as pd

spark = SparkSession.builder            .appName('Python Spark Postgresql')            .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0")            .config("spark.sql.execution.arrow.enabled", "true")            .getOrCreate()

    