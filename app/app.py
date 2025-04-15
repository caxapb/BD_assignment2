from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from cassandra import ConsistencyLevel
import sys
import subprocess


def create_schema(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS search_engine 
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
    """)
    
    session.execute("""
        CREATE TABLE IF NOT EXISTS search_engine.terms (
            term text PRIMARY KEY,
            df int
        );
    """)

    session.execute("""
        CREATE TABLE IF NOT EXISTS search_engine.term_frequencies (
            term text,
            doc_id int,
            tf int,
            PRIMARY KEY (term, doc_id)
        );
    """)

    session.execute("""
        CREATE TABLE IF NOT EXISTS search_engine.documents (
            doc_id int PRIMARY KEY,
            length int
        );
    """)

    session.execute("""
        CREATE TABLE IF NOT EXISTS search_engine.stats (
            key text PRIMARY KEY,
            value float
        );
    """)


def index_data(session):
    # Read from reducer output
    # batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    # batch_size = 0
    # max_batch_size = 50
    proc = subprocess.Popen(
        ['hdfs', 'dfs', '-cat', '/tmp/index_output/part-00000'],
        stdout=subprocess.PIPE,
        universal_newlines=True
    )
    for line in proc.stdout:
        # print(type(line))
        # proc.wait()
        line = line.strip().split('\t')
        if line[0] == 'tf':
            term, doc_id, tf = line[1], int(line[2]), int(line[3])
            session.execute(
                "INSERT INTO search_engine.term_frequencies (term, doc_id, tf) VALUES (%s, %s,%s)",
                (term, doc_id, tf)
            )
        elif line[0] == 'df':
            term, docs = line[1], int(line[2])
            session.execute(
                "INSERT INTO search_engine.terms (term, df) VALUES (%s, %s)",
                (term, docs)
            )
        elif line[0] == 'doc_stats':
            doc_id, length = int(line[1]), int(line[2])
            session.execute(
                "INSERT INTO search_engine.documents (doc_id, length) VALUES (%s, %s)",
                (doc_id, length)
            ) 
        elif line[0] == 'global_stats':
            metric, value = line[1], float(line[2])
            session.execute(
                "INSERT INTO search_engine.stats (key, value) VALUES (%s, %s)",
                (metric, value)
            )
    proc.wait()
            # doc_id = parts[0]
            # if parts[1] == 'length':
            #     length = int(parts[2])
                # session.execute(
                #     "INSERT INTO document_stats (doc_id, length) VALUES (%s, %s)",
                #     (doc_id, length)
                # )
            # elif parts[1] == 'title':
            #     title = parts[2]
            #     session.execute(
            #         "UPDATE document_stats SET title = %s WHERE doc_id = %s",
            #         (title, doc_id)
            #     )
            # continue
            
    #     term, count = line.strip().split('\t')
        
    #     # Get document info for this term
    #     doc_id = count.split(':')[0]
    #     tf = int(count.split(':')[1])
        
    #     # Get title from previously stored data
    #     title_row = session.execute(
    #         "SELECT title FROM document_stats WHERE doc_id = %s", (doc_id,)
    #     ).one()
    #     title = title_row.title if title_row else ""
        
    #     batch.add(
    #         "INSERT INTO inverted_index (term, doc_id, tf, title) VALUES (%s, %s, %s, %s)",
    #         (term, doc_id, tf, title)
    #     )
    #     batch_size += 1
        
    #     if batch_size >= max_batch_size:
    #         session.execute(batch)
    #         batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    #         batch_size = 0
    
    # if batch_size > 0:
    #     session.execute(batch)

if __name__ == "__main__":
    cluster = Cluster(['cassandra-server'])
    session = cluster.connect()
    
    create_schema(session)
    index_data(session)
    
    session.shutdown()
    cluster.shutdown()