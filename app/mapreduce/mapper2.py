import sys

for line in sys.stdin:
    doc_id, title, content = line.strip().split('\t')
    terms = content.lower().split()
    print(f"{doc_id}\t{len(terms)}")