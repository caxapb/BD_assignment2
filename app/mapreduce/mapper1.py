import sys
import re

for line in sys.stdin:
    doc_id, title, content = line.strip().split('\t')

    terms = content.lower()
    terms = re.findall(r'\b\w+\b', terms)

    doc_length = len(terms)
    
    # Emit term frequencies
    for term in terms:
        print(f"TF\t{term}\t{doc_id}")
    
    # Emit document length
    print(f"LEN\t{doc_id}\t{doc_length}")