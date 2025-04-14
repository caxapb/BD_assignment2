from pathvalidate import sanitize_filename
from tqdm import tqdm
from pyspark.sql import SparkSession
import os


spark = SparkSession.builder \
    .appName('data preparation') \
    .master("local") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.parquet.enableVectorizedReader", "true") \
    .getOrCreate()


df = spark.read.parquet("/test.parquet")

n = 10
# df = df.select(['id', 'title', 'text']).sample(fraction=100 * n / df.count(), seed=0).limit(n)
df = df.select(['id', 'title', 'text'])


def create_doc(row):
    filename = "data/" + sanitize_filename((str(row['id']) + "_" + row['title']).split('.')[0]).replace(" ", "_") + ".txt"
    with open(filename, "w") as f:
        f.write(row['text'])


df.foreach(create_doc)
