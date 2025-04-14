from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from cassandra import ConsistencyLevel
import sys

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
    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    batch_size = 0
    max_batch_size = 50
    
    for line in sys.stdin:
        if line.startswith('!'):
            # Document metadata
            parts = line.strip().split('\t')
            doc_id = parts[0][1:]
            if parts[1] == 'length':
                length = int(parts[2])
                session.execute(
                    "INSERT INTO document_stats (doc_id, length) VALUES (%s, %s)",
                    (doc_id, length)
                )
            elif parts[1] == 'title':
                title = parts[2]
                session.execute(
                    "UPDATE document_stats SET title = %s WHERE doc_id = %s",
                    (title, doc_id)
                )
            continue
            
        term, count = line.strip().split('\t')
        
        # Get document info for this term
        doc_id = count.split(':')[0]
        tf = int(count.split(':')[1])
        
        # Get title from previously stored data
        title_row = session.execute(
            "SELECT title FROM document_stats WHERE doc_id = %s", (doc_id,)
        ).one()
        title = title_row.title if title_row else ""
        
        batch.add(
            "INSERT INTO inverted_index (term, doc_id, tf, title) VALUES (%s, %s, %s, %s)",
            (term, doc_id, tf, title)
        )
        batch_size += 1
        
        if batch_size >= max_batch_size:
            session.execute(batch)
            batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
            batch_size = 0
    
    if batch_size > 0:
        session.execute(batch)

if __name__ == "__main__":
    cluster = Cluster(['cassandra-server'])
    session = cluster.connect()
    
    create_schema(session)
    # index_data(session)
    
    # print("Indexing completed successfully!")
    # cluster.shutdown()