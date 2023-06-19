
import os
from pyspark.sql import SparkSession
import boto3
import pandas as pd

spark = SparkSession.builder            .appName('Python Spark')            .config("spark.sql.execution.arrow.enabled", "true")            .getOrCreate()



column_names = None
condition = None

df_source1 = spark.read    .format('jdbc')    .option('url', 'jdbc:oracle:thin:@//database-2.cabfbspytumf.us-west-2.rds.amazonaws.com:1521/TESTING')    .option('dbtable', 'employees')    .option('user', 'oracleetl')    .option('password', 'oracleetl')    .option('driver', 'oracle.jdbc.driver.OracleDriver')     .load()

df_source1 = df_source1.toPandas()

if column_names is not None:
    df_source1 = df_source1[column_names]

if condition is not None:
    df_source1 = df_source1.query(condition)


    