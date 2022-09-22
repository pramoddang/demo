import os
import sys
import pyspark
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
spark = SparkSession.builder.appName("test").getOrCreate()

df = spark.read.csv(r"C:\Users\dangp\PycharmProjects\pythonProject4\employee_data.csv",inferSchema=True,header=True)
# df.show()
# print(df.columns)
# print(df.describe().show())
# df.select('age').show()
# print(df.head(5))

df2 = df.withColumn("Monthly_Salary",df['salary']/12)
df3 = df2.withColumnRenamed('salary','yearly_salary')
df3.show()


structureData = [
    (("James","","Smith"),"36636","M",3100),
    (("Michael","Rose",""),"40288","M",4300),
    (("Robert","","Williams"),"42114","M",1400),
    (("Maria","Anne","Jones"),"39192","F",5500),
    (("Jen","Mary","Brown"),"","F",-1)
  ]
structureSchema = StructType([
        StructField('name', StructType([
             StructField('firstname', StringType(), True),
             StructField('middlename', StringType(), True),
             StructField('lastname', StringType(), True)
             ])),
         StructField('id', StringType(), True),
         StructField('gender', StringType(), True),
         StructField('salary', IntegerType(), True)
         ])