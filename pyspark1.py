import os
import sys
import pyspark
from pyspark.sql import SparkSession
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark1 = SparkSession.builder.appName("MyFirstApp").getOrCreate()

columns = ["language","users_count"]
data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]

rdd = spark1.sparkContext.parallelize(data)

df = rdd.toDF()
df.printSchema()
df1 = rdd.toDF(columns)
df1.printSchema()

df2 = spark1.createDataFrame(rdd).toDF(*columns)
df2.show()