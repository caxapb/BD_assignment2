from pyspark.sql import SparkSession


spark = SparkSession.builder \
    .appName('Data preparation, rdd creation') \
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
    # extract titles doc_id from filenames and remove underscore between id and title
    filename = filename.split('/')[-1].rsplit('.', 1)[0]
    filename = filename.split('_', 1)
    doc_id = filename[0]
    title = filename[1] if len(filename) > 1 else ''
    # also remove underscores in titles
    title = title.replace('_', ' ')
    return f"{doc_id}\t{title}\t{text}"

parsed_rdd = rdd.map(parse_file)
parsed_rdd.collect()
parsed_rdd.saveAsTextFile("/index/data")
