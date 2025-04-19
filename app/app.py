from cassandra.cluster import Cluster
import subprocess


def create_keyspace(session):
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
            length int,
            title text
        );
    """)

    session.execute("""
        CREATE TABLE IF NOT EXISTS search_engine.stats (
            key text PRIMARY KEY,
            value float
        );
    """)


def insert_data(session):
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
            doc_id, length, title = int(line[1]), int(line[2]), line[3]
            session.execute(
                "INSERT INTO search_engine.documents (doc_id, length, title) VALUES (%s, %s, %s)",
                (doc_id, length, title)
            ) 
        elif line[0] == 'global_stats':
            metric, value = line[1], float(line[2])
            session.execute(
                "INSERT INTO search_engine.stats (key, value) VALUES (%s, %s)",
                (metric, value)
            )
    proc.wait()

if __name__ == "__main__":
    cluster = Cluster(['cassandra-server'])
    session = cluster.connect()
    
    create_keyspace(session)
    insert_data(session)
    
    session.shutdown()
    cluster.shutdown()