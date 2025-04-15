from pyspark.sql import SparkSession


spark = SparkSession.builder \
    .appName('data preparation') \
    .master("local") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.parquet.enableVectorizedReader", "true") \
    .getOrCreate()

sc = spark.sparkContext

path = "/data"
rdd = sc.wholeTextFiles(path)

def parse_file(pair):
    filename, text = pair

    base_name = filename.split('/')[-1].rsplit('.', 1)[0]
    name = base_name.split('_', 1)
    doc_id = name[0]
    title_part = name[1] if len(name) > 1 else ''

    title = title_part.replace('_', ' ')
    return f"{doc_id}\t{title}\t{text}"

parsed_rdd = rdd.map(parse_file)

parsed_rdd.collect()

parsed_rdd.saveAsTextFile("/index/data")
