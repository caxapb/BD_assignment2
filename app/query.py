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
# from cassandra.cluster import Cluster

import sys
import math
import re

def tokenize(qr):
    return re.findall(r'\b\w+\b', qr)


# Constants for BM25
K1 = 1
B = 0.75

def calculate_idf(df, total_docs):
    """Calculate IDF using the formula."""
    return math.log((total_docs) / (df) + 0.001)

def calculate_bm25(tf, idf, doc_length, avgdl):
    """Calculate BM25 score for a single term in a document."""
    numerator = tf * (K1 + 1)
    denominator = tf + K1 * (1 - B + B * (doc_length / avgdl))
    return idf * (numerator / denominator)

def compute_bm25_scores(joined_data):
    doc_id, (term_freqs, doc_length) = joined_data
    try:
        score = sum(
            calculate_bm25(term_freqs[term], idf_map[term], doc_length, avg_length)
            for term in query_terms)
        print('!'*100, 'SCORE')
        print(score)
        return (doc_id, float(score))
    except Exception as e:
        print(f"Error calculating BM25 for doc {doc_id}: {str(e)}")
        return (doc_id, 0.0)

def main():
    spark = SparkSession.builder \
        .appName("TFIDF search") \
        .config("spark.cassandra.connection.host", "cassandra-server") \
        .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.4.0") \
        .getOrCreate()


    query = tokenize(sys.argv[1].lower())
    
    # Load global statistics from Cassandra
    stats_df = spark.read.format("org.apache.spark.sql.cassandra") \
        .options(table="stats", keyspace="search_engine").load()
    
    stats = {row['key']: row['value'] for row in stats_df.collect()}
    N = stats.get('docs_total', 0)
    avg_length = stats.get('avg_length', 0.0)
    
    # term - df - how many docs contain term
    terms_df = spark.read.format("org.apache.spark.sql.cassandra") \
        .options(table="terms", keyspace="search_engine") \
        .load()
    
    # term - doc-id - tf - how many times term is met in doc_id
    term_frequencies_df = spark.read.format("org.apache.spark.sql.cassandra") \
        .options(table="term_frequencies", keyspace="search_engine") \
        .load()
    
    # dic-id - length
    documents_df = spark.read.format("org.apache.spark.sql.cassandra") \
        .options(table="documents", keyspace="search_engine") \
        .load()
    
    print('!'*100)
    print(N, avg_length)
    
    # # Load terms (vocabulary) from Cassandra
    
    terms_rdd = terms_df.rdd.map(lambda row: (row["term"], row["df"]))
    print('!'*100)
    print('terms_rdd')
    print(terms_rdd.take(5))
    # Calculate IDF for query terms
    query_terms = set(query)
    idf_map = terms_rdd.filter(lambda x: x[0] in query_terms) \
        .mapValues(lambda df: calculate_idf(df, N)).collectAsMap()
    print('!'*100)
    print('idf')
    print(idf_map)


    # Filter term frequencies for query terms
    term_frequencies_rdd = term_frequencies_df.rdd \
        .filter(lambda row: row["term"] in query_terms) \
        .map(lambda row: (row["doc_id"], {row["term"]: row["tf"]}))
    print('!'*100)
    print('term freq rdd')
    print(term_frequencies_rdd.take(5))
    # Join with document metadata
    documents_rdd = documents_df.rdd.map(lambda row: (row["doc_id"], row["length"]))
    print('!'*100)
    print('doc rdd')
    print(documents_rdd.take(5))

    print('!'*100)
    print('ALL RDD ARE COLLECTED')

    bm25_scores = term_frequencies_rdd.join(documents_rdd).map(compute_bm25_scores)
    print('!'*100)
    print('BM SCORES ARE COMPUTED')
    print(bm25_scores.take(5))
    print('!'*100)
    print('UPPER STRING IS BM25')


    # Collect top 10 results
    top_10 = bm25_scores.takeOrdered(10, key=lambda x: -x[1])
    
    # Retrieve document titles
    doc_titles = documents_df.filter(col("doc_id").isin([doc_id for doc_id, _ in top_10])) \
        .rdd.map(lambda row: (row["doc_id"], row["title"])).collectAsMap()

    print('!'*100)
    print('PRINTING')

    # Print results
    for doc_id, score in top_10:
        title = doc_titles.get(doc_id, "Unknown Title")
        print(f"Document ID: {doc_id}, Title: {title}, Score: {score:.4f}")

    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    main()