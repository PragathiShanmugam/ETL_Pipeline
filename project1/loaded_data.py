
import os
from pyspark.sql import SparkSession
import boto3
import pandas as pd

spark = SparkSession.builder            .appName('Python Spark Postgresql')            .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0")            .config("spark.sql.execution.arrow.enabled", "true")            .getOrCreate()

jdbc_url = 'jdbc:postgresql://agilisium-innovation-lab.cabfbspytumf.us-west-2.rds.amazonaws.com:5432/Automated_Data_Lake'
table_name = 'test_table1'
username = 'postgres'
password = 'Kaa40N2oi!#Q'
file_name = 'clienttest.csv'
bucket_name = 'etltestingdemo'
object_name = 'Notebooks/Input_File/'

# Load the data from S3
s3_client = boto3.client('s3',
                         aws_access_key_id='ASIAXWDKBRAVQULSK2EJ',
                         aws_secret_access_key='RYlUa8g2pWLr7ByN6OYZXZWVsoYsdceZ5tkvkMft',
                         aws_session_token='IQoJb3JpZ2luX2VjECsaCXVzLXdlc3QtMiJHMEUCIQDiaA9RWWmQFn13mrofeSMHBr+oZvqJbW6fEUpON+irlwIgdKw5XN6GK7GYBfPR/vbJvzpdpC/bWPklN+dUzQBt9x4qnwMIdBAAGgw1Mjg1MDMzNzU5MTUiDAWwUkDan4kdZFDrNSr8Au4zFt+78t20/mEGV35kRBGcV5ENV+j0+7n9+9vAvbyRtnFv77v0X/N5Mnp/hoTyHgAD3ctqrLNH5aGbP7SkZaUDkwF6acWDcTvuAODFLtxgSwVJ4JOonVyBrwrhhYzc/AXuzIV27J7SomeANEBtZVJzeRzNT92vFrqdtEv4g4eVizvTS7KNJFQVLb0CnKheYrvANLZyD7CEZOqrOkOKoHcJ5bNtflpoEbAkOtDTNg2Ubz2w+DJ2iGn0J1oK8JxdREug8gtvcC89iqQkVcQn1LWuSUCCDlll7eruXVC4Uu7624f2wrG5C/nLumLqIrjEeNSMuRBmvs4zWS3k1liSLzfISiy25kKLmOPryc3vJFJkQ/fl2QEDEtn0gD1n5AACyTw3uBQnAodPm1dyNvasuU4q3opggaM+VTwB5VYkIhla1pmeFDGIXwlV3JkYRfG4KMix9WDO6Paey62owZlpLX40QARGwf1PXaHoHEnWp1ljzWxc30dPQFrRlhQDMMXehqQGOqYBuuJzWoEm/QFb46m1PfuXRDGySmrI5bQ86iu/9c+eKDkDAylRjTZF3+fYDQMT8iNqA952fLxSAADNRDLOnHMEaekI3Q+EvGqGJL5JcwAtxPbnFhicQA7+5E5ADFDTAc0euTG330FnJ1TaMhEBgwmX4YmzUT3Anlm8lWcuuB/QuXpaB1D9MwBBTWG4UuPYHqQMrYzyqJNgMx30inIFaAigSgBAc+Pbhw==',
                         region_name='us-west-2')
object_name_with_file = os.path.join(object_name, file_name)
s3_object = s3_client.get_object(Bucket=bucket_name, Key=object_name_with_file)
data = pd.read_csv(s3_object['Body'])

# Convert pandas DataFrame to PySpark DataFrame
df = spark.createDataFrame(data)

# Write the DataFrame to the database table
df.write.format('jdbc')     .option('url', jdbc_url)     .option('dbtable', table_name)     .option('user', username)     .option('password', password)     .option('driver', 'org.postgresql.Driver')     .mode('append')     .save()

spark.stop()

    