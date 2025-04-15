# import sys
# from collections import defaultdict

# term_tf = defaultdict(int)
# term_docs = defaultdict(set)  # Track unique docs per term
# doc_lengths = {}

# for line in sys.stdin:
#     parts = line.strip().split('\t')
    
#     if parts[0] == "TF":
#         term, doc_id, count = parts[1], parts[2], int(parts[3])
#         term_tf[(term, doc_id)] += count
        
#         # Track document frequency (only once per term-doc pair)
#         if (term, doc_id) not in term_tf:  # First occurrence of term in this doc
#             term_docs[term].add(doc_id)
    
#     elif parts[0] == "LEN":
#         doc_id, length = parts[1], int(parts[2])
#         doc_lengths[doc_id] = length

# avg_length = sum(doc_lengths.values()) / len(doc_lengths) if doc_lengths else 0

# # Outputs remain the same...

# # Output format:
# # [table_type]\t[data_fields]

# # 1. Output term_index data (term, doc_id, tf)
# for (term, doc_id), tf in term_tf.items():
#     print(f"term_index\t{term}\t{doc_id}\t{tf}")

# # 2. Output term_df data (term, document frequency)
# for term, docs in term_docs.items():
#     print(f"term_df\t{term}\t{len(docs)}")

# # 3. Output doc_stats data (doc_id, length)
# for doc_id, length in doc_lengths.items():
#     print(f"doc_stats\t{doc_id}\t{length}")

# # 4. Output average document length
# print(f"avg_doc_length\t{avg_length}")


import sys
from collections import defaultdict

tf = defaultdict(int)  # how many times word x appears in doc d
df = defaultdict(set)  # Track unique docs per term
doc_lengths = {}

for line in sys.stdin:
    parts = line.strip().split('\t')
    
    if parts[0] == "TF":
        term, doc_id = parts[1], parts[2]
        if (term, doc_id) not in tf:
            df[term].add(doc_id)
        tf[(term, doc_id)] += 1
        
    
    elif parts[0] == "LEN":
        doc_id, length = parts[1], int(parts[2])
        doc_lengths[doc_id] = length

avg_length = sum(doc_lengths.values()) / len(doc_lengths) if doc_lengths else 0


# Output format:
# [table_type]\t[data_fields]

# 1. Output term_index data (term, doc_id, tf)
for (term, doc_id), tf in tf.items():
    print(f"tf\t{term}\t{doc_id}\t{tf}")

# 2. Output term_df data (term, document frequency)
for term, docs in df.items():
    print(f"df\t{term}\t{len(docs)}")

# 3. Output doc_stats data (doc_id, length)
for doc_id, length in doc_lengths.items():
    print(f"doc_stats\t{doc_id}\t{length}")

# 4. Output average document length
print(f"global_stats\t{'docs_total'}\t{len(doc_lengths)}")
print(f"global_stats\t{'avg_length'}\t{avg_length}")