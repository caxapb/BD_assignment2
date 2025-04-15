from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import sys
import math
import re

K1 = 1
B = 0.75

def tokenize(qr):
    return re.findall(r'\b\w+\b', qr)

def calculate_idf(df, total_docs):
    return math.log((total_docs) / (df) + 0.001)

def calculate_bm25(tf, idf, doc_length, avgdl):
    numerator = tf * (K1 + 1)
    denominator = tf + K1 * (1 - B + B * (doc_length / avgdl))
    return idf * (numerator / denominator)

def compute_bm25_scores(joined_data):
    doc_id, (term_freqs, doc_length) = joined_data
    try:
        score = sum(
            calculate_bm25(term_freqs[term], idf_map[term], doc_length, avg_length)
            for term in query)
        print('!'*100, 'SCORE')
        print(score)
        return (doc_id, float(score))
    except:
        return (doc_id, 0.0)

def main(filename):
    spark = SparkSession.builder \
        .appName("TFIDF search") \
        .config("spark.cassandra.connection.host", "cassandra-server") \
        .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.4.0") \
        .getOrCreate()


    query = tokenize(sys.argv[1].lower())
    query = set(query)

    stats_df = spark.read.format("org.apache.spark.sql.cassandra") \
        .options(table="stats", keyspace="search_engine").load()
    
    stats = {row['key']: row['value'] for row in stats_df.collect()}
    N = stats.get('docs_total', 0)
    avg_length = stats.get('avg_length', 0.0)
    
    # term - df - how many docs contain term
    terms_df = spark.read.format("org.apache.spark.sql.cassandra") \
        .options(table="terms", keyspace="search_engine") \
        .load()
    terms_rdd = terms_df.rdd.map(lambda row: (row["term"], row["df"]))

    # term - doc-id - tf - how many times term is met in doc_id
    term_frequencies_df = spark.read.format("org.apache.spark.sql.cassandra") \
        .options(table="term_frequencies", keyspace="search_engine") \
        .load()
    term_frequencies_rdd = term_frequencies_df.rdd \
        .filter(lambda row: row["term"] in query) \
        .map(lambda row: (row["doc_id"], {row["term"]: row["tf"]}))
    
    # doc-id - length
    documents_df = spark.read.format("org.apache.spark.sql.cassandra") \
        .options(table="documents", keyspace="search_engine") \
        .load()
    documents_rdd = documents_df.rdd.map(lambda row: (row["doc_id"], row["length"]))

    idf_map = terms_rdd.filter(lambda x: x[0] in query) \
        .mapValues(lambda df: calculate_idf(df, N)).collectAsMap()
    

    bm25 = term_frequencies_rdd.join(documents_rdd).map(compute_bm25_scores)
    top = bm25.takeOrdered(10, key=lambda x: -x[1])
    
    doc_titles = documents_df.filter(col("doc_id").isin([doc_id for doc_id, _ in top])) \
        .rdd.map(lambda row: (row["doc_id"], row["title"])).collectAsMap()

    with open(filename, 'w') as file:
        for doc_id, score in top:
            title = doc_titles.get(doc_id, "Unknown")
            print(f"Document: {doc_id}, title: {title}, score: {score:.4f}")
            file.write(f"Document: {doc_id}, title: {title}, score: {score:.4f}.\n")

    spark.stop()

if __name__ == "__main__":
    filename = 'output.txt'
    main(filename)