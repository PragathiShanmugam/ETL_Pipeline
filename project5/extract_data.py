
import os
from pyspark.sql import SparkSession
import boto3
import pandas

spark = SparkSession.builder            .appName('Python Spark Postgresql')            .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0")            .config("spark.sql.execution.arrow.enabled", "true")            .getOrCreate()

jdbc_url = 'jdbc:postgresql://agilisium-innovation-lab.cabfbspytumf.us-west-2.rds.amazonaws.com:5432/Automated_Data_Lake'
table_name = 'storage'
username = 'postgres'
password = 'Kaa40N2oi!#Q'
file_name = 'dbcontents2.csv'
bucket_name = 'etltestingdemo'
object_name = 'Input_File/'
column_names = None
condition = 'storage_id>10'

df = spark.read    .format('jdbc')    .option('url', jdbc_url)    .option('dbtable', table_name)    .option('user', username)    .option('password', password)    .option('driver', 'org.postgresql.Driver')    .load()

df = df.toPandas()

if column_names is not None:
    df = df[column_names]

if condition is not None:
    df = df[df.eval(condition)]

file_ext = os.path.splitext(file_name)[1]

if file_ext == '.parquet':
    df.to_parquet(file_name)
elif file_ext == '.csv':
    df.to_csv(file_name, header=True, index=False)
else:
    raise ValueError("Invalid file type specified")

client = boto3.client('s3',aws_access_key_id='ASIAXWDKBRAV6BXYJRPD',
                        aws_secret_access_key='+Gr1pl5iIXeLNf8qMfnx8GlSjUwlLTlyxxbNyWgm',
                    aws_session_token='IQoJb3JpZ2luX2VjEEQaCXVzLXdlc3QtMiJGMEQCIHSNPbSdtMd+NPkvdQaeqPku+IOF6iMnljeER64aY5g3AiBAbq4QGYn5mFkqgPZsFwb7CoHUpYkteo3OBO04gvqHNSqjAwhtEAAaDDUyODUwMzM3NTkxNSIMsUglZK76UDG9VI+kKoADR/AX44hAOPypXwdS+9Yu0swCNO9wHmyaV7EW9TzE+SdsXWXfSm0HkMBu+BryEOK/U/S3SYmjkJsT8mlsAkA2/fsJPWhodJvXuap5y36XOx8dvAQZ/l21uRjnCDbTM7l7g758zNbBHQcy7oyz1KoulzSXz5AWaLKJ1BcORbPQmdJvOL+HJlUDc40j2QtJ8+iEzHrbbDjm+UNXD8g4LK9tIiKOUbwWoZWBJIRWOjSZBbMky53C8Z02F0g9N9momrcrWg+lqxkBiSyCcmzRscgfKidGefaPq+nJdQJhA8pMz/bdYL6AwDjUrLA1zQ3he53L7zYtTmbmspPxP0Nv8ohizMLzr0eBTQXKIK2HWxV+W3mKxJgSzfspDtfUfvHiw2Xzyeay+PLhEgVVv91pUINwlPiL8kwazZTzNPptWDoWN8oG09U8rIXydHyKWWPw0sOJkCLrK4Cl1OEuJUI+ex7m7i9dn0MuxQQHM0VKx+R0QF5tmNQoSHMu7SLQ/wj0HcmHMPvlm6MGOqcBw9umfDAfF4PtpQ/1FEEdLWdQ8asyb1Y+9r4nB2DHqjZ3TLZjbNeK5BjR9dZ7+j9fVFnRdg7m1LmK9EFxI4uI7phT/RVxALn3jUmJdTXPdchFaCTJvKEQdrT8mnyrjJakQmeAWmfBOMnswogU9NNgVS0e2PxkptITmz7gnHh4niMddgMuAoxAsnRo5oc4DU4VoKokgTfgfwAa5ywTyiMF1pcusdWmA1o=',
                          region_name='us-west-2')

object_name_with_file = os.path.join(object_name, file_name)

try:
    client.head_object(Bucket=bucket_name, Key=object_name_with_file)
    raise ValueError("File already exists. Please enter a new name.")
except client.exceptions.ClientError as e:
    if e.response['Error']['Code'] == '404':
        response = client.upload_file(file_name, bucket_name, object_name_with_file)
        print(response)

spark.stop()
