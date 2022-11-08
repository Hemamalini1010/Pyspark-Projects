from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import *
#Define inbound and outbound folders
inbound_file='gs://sample_test_hema/part-00000-d326e1da-15af-4cb2-8f5d-b8d04887d522-c000.csv'
outbound_file='gs://sample_test_hema/part-new'
#Create Spark Session
spark = SparkSession.builder.appName('DataProc-PySpark-Test').getOrCreate()
#Read inbound file
df = spark.read.format('csv').load(inbound_file)
#Transform and save
df.write.mode('append').format('parquet').save(outbound_file)