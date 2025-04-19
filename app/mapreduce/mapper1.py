import sys
import re

def main():
    for line in sys.stdin:
        doc_id, title, content = line.strip().split('\t')

        terms = content.lower()
        terms = re.findall(r'\b\w+\b', terms)

        doc_length = len(terms)
        
        for term in terms:
            print(f"TF\t{term}\t{doc_id}")
        
        # Emit document length and title
        print(f"DOC\t{doc_id}\t{doc_length}\t{title}")


if __name__ == "__main__":
    main()