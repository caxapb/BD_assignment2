import sys
from collections import defaultdict

def main():
    tf = defaultdict(int)  # how many times word x appears in doc d
    df = defaultdict(set)  # track unique docs per term
    doc_data = {}
    lengths_sum = 0

    for line in sys.stdin:
        parts = line.strip().split('\t')
        
        if parts[0] == "TF":
            term, doc_id = parts[1], parts[2]
            # if terms wasn't met in this doc_id: update df value
            if (term, doc_id) not in tf:
                df[term].add(doc_id)
            # update tf value
            tf[(term, doc_id)] += 1
            
        
        elif parts[0] == "DOC":
            doc_id, length, title = parts[1], int(parts[2]), parts[3]
            # save length and title of each doc_id
            doc_data[doc_id] = (length, title)
            lengths_sum += length

    avg_length = lengths_sum / len(doc_data) if doc_data else 0

    for (term, doc_id), tf in tf.items():
        print(f"tf\t{term}\t{doc_id}\t{tf}")

    for term, docs in df.items():
        print(f"df\t{term}\t{len(docs)}")

    for doc_id, (length, title) in doc_data.items():
        print(f"doc_stats\t{doc_id}\t{length}\t{title}")

    print(f"global_stats\t{'docs_total'}\t{len(doc_data)}")
    print(f"global_stats\t{'avg_length'}\t{avg_length}")

if __name__ == "__main__":
    main()