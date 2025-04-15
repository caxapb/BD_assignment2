# import sys
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, log, sum as spark_sum
# from pyspark.sql.types import FloatType
# import math
# import re

# # BM25 parameters
# K1 = 1
# B = 0.75
# def tokenize(qr):
#     return re.findall(r'\b\w+\b', qr)

# def main(query):
#     # Initialize Spark Session with Cassandra config
#     spark = SparkSession.builder \
#         .appName("TFIDF search") \
#         .config("spark.cassandra.connection.host", "cassandra") \
#         .config("spark.cassandra.auth.username", "cassandra") \
#         .config("spark.cassandra.auth.password", "cassandra") \
#         .getOrCreate()

#     # Read data from Cassandra
#     terms_df = spark.read \
#         .format("org.apache.spark.sql.cassandra") \
#         .options(table="terms", keyspace="search_engine") \
#         .load()
    
#     term_freq_df = spark.read \
#         .format("org.apache.spark.sql.cassandra") \
#         .options(table="term_frequencies", keyspace="search_engine") \
#         .load()
    
#     documents_df = spark.read \
#         .format("org.apache.spark.sql.cassandra") \
#         .options(table="documents", keyspace="search_engine") \
#         .load()
    
#     stats_df = spark.read \
#         .format("org.apache.spark.sql.cassandra") \
#         .options(table="stats", keyspace="search_engine") \
#         .load()

#     # Extract stats values
#     stats = {row['key']: row['value'] for row in stats_df.collect()}
#     N = stats.get('docs_total', 0)
#     avg_length = stats.get('avg_length', 0.0)

#     # Process query terms
#     terms = tokenize(query)
#     if not terms:
#         print("No valid query terms")
#         spark.stop()
#         return

#     # Get term frequencies for query terms
#     query_term_freq = term_freq_df.filter(col("term").isin(terms))

#     # Get document frequencies for query terms
#     query_terms_df = terms_df.filter(col("term").isin(terms))

#     # Calculate IDF for each term
#     idf_df = query_terms_df.withColumn(
#         "idf", 
#         log((N - col("df") + 0.5) / (col("df") + 0.5) + 1)
#     )

#     # Join with document lengths
#     joined_df = query_term_freq.join(
#         documents_df, "doc_id"
#     ).join(
#         idf_df, "term"
#     )

#     # Calculate BM25 score
#     def compute_bm25(tf, length, idf):
#         numerator = tf * (K1 + 1)
#         denominator = tf + K1 * (1 - B + B * (length / avg_length))
#         return float(idf * numerator / denominator)

#     from pyspark.sql.functions import udf
#     bm25_udf = udf(compute_bm25, FloatType())

#     # Compute scores for each document
#     scores_df = joined_df.withColumn(
#         "bm25", 
#         bm25_udf(col("tf"), col("length"), col("idf"))
#     ).groupBy("doc_id").agg(
#         spark_sum("bm25").alias("score")
#     ).orderBy(
#         col("score").desc()
#     ).limit(10)

#     # Read document titles from HDFS
#     def extract_id_title(path):
#         filename = path.split("/")[-1]
#         doc_id = int(filename.split("_")[0])
#         title = "_".join(filename[:-4].split("_")[1:])
#         return (doc_id, title)

#     titles_rdd = spark.sparkContext.wholeTextFiles("hdfs:///data/*.txt") \
#         .map(lambda x: extract_id_title(x[0]))
    
#     titles_df = titles_rdd.toDF(["doc_id", "title"])

#     # Join with titles and collect results
#     results = scores_df.join(
#         titles_df, "doc_id"
#     ).select(
#         "doc_id", "title", "score"
#     ).collect()

#     # Output results
#     print("\nTop 10 relevant documents:")
#     for row in results:
#         print(f"ID: {row['doc_id']}\tTitle: {row['title']}\tScore: {row['score']:.4f}")

#     spark.stop()

# if __name__ == "__main__":
#     if len(sys.argv) < 2:
#         print("Usage: query.py '<query>'")
#         sys.exit(1)

#     main(sys.argv[1])


from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from cassandra.cluster import Cluster

import sys
import math

# Constants for BM25
K1 = 1
B = 0.75

def calculate_idf(df, total_docs):
    """Calculate IDF using the formula."""
    return math.log((total_docs - df + 0.5) / (df + 0.5) + 1)

def calculate_bm25(tf, idf, doc_length, avgdl):
    """Calculate BM25 score for a single term in a document."""
    numerator = tf * (K1 + 1)
    denominator = tf + K1 * (1 - B + B * (doc_length / avgdl))
    return idf * (numerator / denominator)

def main():
    spark = SparkSession.builder \
        .appName("BM25 Search") \
        .config("spark.cassandra.connection.host", "cassandra-server") \
        .getOrCreate()


    query = sys.argv[1].lower().split()

    # Load global statistics from Cassandra
    stats_df = spark.read.format("org.apache.spark.sql.cassandra") \
        .options(table="stats", keyspace="search_engine").load()
    
    total_docs = stats_df.filter(col("key") == "docs_total").select("value").collect()[0]["value"]

    avgdl = stats_df.filter(col("key") == "avg_length").select("value").collect()[0]["value"]

    print('!'*100)
    print(total_docs, avgdl)
    # Load terms (vocabulary) from Cassandra
    terms_df = spark.read.format("org.apache.spark.sql.cassandra") \
        .options(table="terms", keyspace="search_engine").load()
    
    terms_rdd = terms_df.rdd.map(lambda row: (row["term"], row["df"]))

    # Calculate IDF for query terms
    query_terms = set(query)
    idf_map = terms_rdd.filter(lambda x: x[0] in query_terms) \
        .mapValues(lambda df: calculate_idf(df, total_docs)).collectAsMap()

    # Load term frequencies and document metadata
    term_frequencies_df = spark.read.format("org.apache.spark.sql.cassandra") \
        .options(table="term_frequencies", keyspace="search_engine").load()
    documents_df = spark.read.format("org.apache.spark.sql.cassandra") \
        .options(table="documents", keyspace="search_engine").load()

    # Filter term frequencies for query terms
    term_frequencies_rdd = term_frequencies_df.rdd \
        .filter(lambda row: row["term"] in query_terms) \
        .map(lambda row: ((row["doc_id"], row["term"]), row["tf"])) \
        .groupByKey() \
        .mapValues(dict)

    # Join with document metadata
    documents_rdd = documents_df.rdd.map(lambda row: (row["doc_id"], row["length"]))

    # Compute BM25 scores
    bm25_scores = term_frequencies_rdd.join(documents_rdd).flatMap(
        lambda x: [
            (
                x[0],  # doc_id
                sum(
                    calculate_bm25(x[1][0][term], idf_map[term], x[1][1], avgdl)
                    for term in query_terms if term in x[1][0]
                )
            )
        ]
    )

    # Collect top 10 results
    top_10 = bm25_scores.takeOrdered(10, key=lambda x: -x[1])

    # Retrieve document titles
    doc_titles = documents_df.filter(col("doc_id").isin([doc_id for doc_id, _ in top_10])) \
        .rdd.map(lambda row: (row["doc_id"], row["title"])).collectAsMap()

    # Print results
    for doc_id, score in top_10:
        title = doc_titles.get(doc_id, "Unknown Title")
        print(f"Document ID: {doc_id}, Title: {title}, Score: {score:.4f}")

    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    main()