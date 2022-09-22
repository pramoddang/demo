import os
import sys
import pyspark
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
import pandas as pd

spark = SparkSession.builder.appName("read").getOrCreate()

df = pd.read_csv(r"C:\Users\dangp\PycharmProjects\pythonProject4\Book1.csv",sep=',',dtype={'a': str})

df1 = pd.read_csv(r"C:\Users\dangp\PycharmProjects\pythonProject4\World_Bank_Projects_downloaded_9_21_2022.csv",header=1 )

with open("C:\Users\dangp\PycharmProjects\pythonProject4\Book1.csv") as f:

    lis = [line.split() for line in f]
    print(lis)
    for i,x in enumerate(lis):
        print("line{0} = {1}".format(i,x))
