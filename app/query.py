from pyspark.sql import SparkSession
import sys
import math
import re

K1 = 1
B = 0.75

def tokenize(qr):
    return re.findall(r'\b\w+\b', qr)

# functions to map RDD objects:
def calculate_idf(df, total_docs):
    return math.log((total_docs) / (df) + 0.001)

def calculate_bm25(doc):
    doc_id, title, doc_length, tf, idf = doc
    score = idf * ( tf * (K1 + 1) / (K1 * (1 - B + B * (doc_length / avg_length)) + tf))
    return (doc_id, title, score)

def main(filename):
    # open the Spark session, connecting to the Cassandra
    spark = SparkSession.builder \
        .appName("TFIDF search") \
        .config("spark.cassandra.connection.host", "cassandra-server") \
        .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.4.0") \
        .getOrCreate()

    # process the query:
    query = tokenize(sys.argv[1].lower())
    query = set(query)

    # if the check from the script is passed (something is passed as an argument)
    # but the argument doesn't contain valid words (w+ from regex)
    # raise the error
    if not query:
        raise "There are no valid terms in query. Input the query which contain words." 

    # read the stats table to get statistical values: number of documents and average doc length
    stats = spark.read.format("org.apache.spark.sql.cassandra") \
        .options(table="stats", keyspace="search_engine").load()
    
    stats = {row['key']: row['value'] for row in stats.collect()}
    N = stats.get('docs_total', 0)
    global avg_length
    avg_length = stats.get('avg_length', 0.0)

    with open(filename, 'a') as file:
        # DEBUG PRINTS ARE COMMENTED
        # ==========================
        file.write(f"Your query after the processing: {query}\n")
        # # file.write(f"N: {N}\n")
        # # file.write(f"avg length: {avg_length}\n")
        
        # term - df - how many docs contain term
        # transform to:
        # [(term, df),...]
        terms = spark.read.format("org.apache.spark.sql.cassandra") \
            .options(table="terms", keyspace="search_engine").load()
        terms_rdd = terms.rdd.map(lambda row: (row["term"], row["df"])).filter(lambda x: x[0] in query)

        # term - doc-id - tf (how many times term is met in doc_id)
        # transform to:
        # [(term, (doc_id, tf)), ...]
        term_frequencies = spark.read.format("org.apache.spark.sql.cassandra") \
            .options(table="term_frequencies", keyspace="search_engine").load()
        term_frequencies_rdd = term_frequencies.rdd \
            .filter(lambda row: row["term"] in query) \
            .map(lambda row: (row["term"], (row["doc_id"], row["tf"])))    
        
        # doc_id - length - title
        # transform to:
        # [doc_id, (length, title)]
        documents = spark.read.format("org.apache.spark.sql.cassandra") \
            .options(table="documents", keyspace="search_engine").load()
        documents_rdd = documents.rdd.map(lambda row: (row["doc_id"], (row["length"], row["title"])))  

        # # file.write(f'terms_rdd:\n {terms_rdd.collect()}\n')
        # # file.write(f'term_frequencies_rdd:\n {term_frequencies_rdd.collect()}\n')
        # # file.write(f'documents_rdd:\n {documents_rdd.collect()}\n')

        # transform to:
        # [(term, idf), ...]
        terms_rdd = terms_rdd.mapValues(lambda df: calculate_idf(df, N))                     
        # # file.write(f"idf_map (term, idf):\n{terms_rdd.collect()}\n")
        
        # transform to:
        # [(term, ((doc_id, tf), idf)), ...]
        joined_data = term_frequencies_rdd.join(terms_rdd)
        # # file.write(f"joined_data:\n{joined_data.collect()}\n")
        
        # transform to:
        # [(doc_id, (term, tf, idf)), ...]
        joined_data = joined_data.map(lambda x: (x[1][0][0], (x[0], x[1][0][1], x[1][1])))
        # # file.write(f"joined_data after map:\n{joined_data.collect()}\n")

        # transform to:
        # [(doc_id, title, doc_length, tf, idf), ...] - the most convenient form to work with (all necessary values))
        # we can simply pass 1 element and extract separate values
        # but first: join with documents_rdd so that we have doc_length values!
        doc_term_info = joined_data.join(documents_rdd) \
            .map(lambda x: (
                x[0],
                x[1][1][1],
                x[1][1][0],
                x[1][0][1],
                x[1][0][2],
            ))
        # # file.write(f"doc_term_info:\n{doc_term_info.collect()}\n")
    
        bm25_scores_per_term = doc_term_info.map(calculate_bm25)
        # # file.write(f"bm25_scores_per_term:\n{bm25_scores_per_term.collect()}\n")

        # now we have scores for doc_id - term pairs, but we need to sum them
        # the formula iterates over terms and  for each doc sum scores from doc-term pairs: combine id-title key and reduceByKey
        doc_scores = bm25_scores_per_term \
            .map(lambda x: ((x[0], x[1]), x[2])) \
            .reduceByKey(lambda a, b: a + b) \
            .map(lambda x: (x[0][0], x[0][1], x[1]))  # recombine back to (doc_id, title, score)
        # # file.write(f"doc_scores:\n{doc_scores.collect()}\n")
        
        # sort by scores (x[2])
        top = doc_scores.sortBy(lambda x: -x[2]).take(10)
        file.write("The list of found papers:\n")
        for doc_id, title, score in top:
            print(f'Document ID: {doc_id}, title: "{title}", score: {score:.2f}')
            file.write(f'Document ID: {doc_id}, title: "{title}", score: {score:.2f}.\n')
        file.write('\n\n')

        file.close()
    spark.stop()

if __name__ == "__main__":
    filename = 'output.txt'
    main(filename)