import os
import sys
import pyspark
from pyspark.sql import SparkSession
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark1 = SparkSession.builder.appName("app").getOrCreate()

# df = spark1.read.csv(r"C:\Users\dangp\PycharmProjects\pythonProject4\rev.csv")
# df.show()

df = spark1.read.text(r"C:\Users\dangp\PycharmProjects\pythonProject4\text1.txt")
df.show()