from pyspark import SparkContext
from cassandra.cluster import Cluster
import math
import sys

def calculate_bm25(session, query, total_docs, avg_length):
    terms = query.lower().split()
    results = {}
    
    # BM25 parameters
    k1 = 1.2
    b = 0.75
    
    for term in terms:
        rows = session.execute(
            "SELECT doc_id, tf, title FROM inverted_index WHERE term = %s", (term,)
        )
        
        df = session.execute(
            "SELECT COUNT(*) FROM inverted_index WHERE term = %s", (term,)
        ).one()[0]
        
        idf = math.log((total_docs - df + 0.5) / (df + 0.5) + 1)
        
        for row in rows:
            doc_id = row.doc_id
            tf = row.tf
            title = row.title
            
            # Get document length
            doc_stats = session.execute(
                "SELECT length FROM document_stats WHERE doc_id = %s", (doc_id,)
            ).one()
            
            if not doc_stats:
                continue
                
            dl = doc_stats.length
            numerator = tf * (k1 + 1)
            denominator = tf + k1 * (1 - b + b * (dl / avg_length))
            score = idf * (numerator / denominator)
            
            if doc_id in results:
                results[doc_id]['score'] += score
            else:
                results[doc_id] = {'title': title, 'score': score}
    
    return results

def main():
    if len(sys.argv) < 2:
        print("Usage: query.py 'search query'")
        return
    
    query = " ".join(sys.argv[1:])
    sc = SparkContext(appName="SearchEngine")
    
    cluster = Cluster(['cassandra-server'])
    session = cluster.connect('search_engine')
    
    # Get corpus statistics
    total_docs = session.execute(
        "SELECT COUNT(*) FROM document_stats"
    ).one()[0]
    
    avg_length = session.execute(
        "SELECT AVG(length) FROM document_stats"
    ).one()[0] or 1.0
    
    # Calculate BM25 scores
    results = calculate_bm25(session, query, total_docs, avg_length)
    
    # Display top 10 results
    sorted_results = sorted(results.items(), key=lambda x: -x[1]['score'])[:10]
    
    print("\nTop 10 Results:")
    for i, (doc_id, data) in enumerate(sorted_results, 1):
        print(f"{i}. {doc_id} - {data['title']} (score: {data['score']:.2f})")
    
    cluster.shutdown()

if __name__ == "__main__":
    main()