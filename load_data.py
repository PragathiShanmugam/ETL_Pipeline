

from pyspark.sql import SparkSession

spark = SparkSession.builder            .appName('Python Spark Postgresql')            .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0")            .config("spark.sql.execution.arrow.enabled", "true")            .config("spark.hadoop.fs.s3a.access.key", "ASIAXWDKBRAVQULSK2EJ")            .config("spark.hadoop.fs.s3a.secret.key", "RYlUa8g2pWLr7ByN6OYZXZWVsoYsdceZ5tkvkMft")            .getOrCreate()
            
jdbc_url = 'jdbc:postgresql://agilisium-innovation-lab.cabfbspytumf.us-west-2.rds.amazonaws.com:5432/Automated_Data_Lake'
table_name = 'test_table1'
username = 'postgres'
password = 'Kaa40N2oi!#Q'
file_name = 'testnew.csv'
bucket_name = 'etltestingdemo'
object_name = 'Notebooks/Input_File'

# Load the data from S3 into a PySpark DataFrame
df = spark.read.csv(f"s3a://etltestingdemo/Notebooks/Input_File/testnew.csv", header=True, inferSchema=True)

# Write the DataFrame to the database table
df.write.format('jdbc')     .option('url', jdbc_url)     .option('dbtable', table_name)     .option('user', username)     .option('password', password)     .option('driver', 'org.postgresql.Driver')     .mode('append')     .save()
print("hello premkumarS")
spark.stop()

    