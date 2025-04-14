import sys
from collections import defaultdict
import json

# Variables to track corpus statistics
current_term = None
term_docs = []
doc_count = 0
doc_lengths = {}
doc_terms = defaultdict(int)

# Process input lines from the mapper
for line in sys.stdin:
    try:
        # Parse the mapper output
        parts = line.strip().split('\t')
        if len(parts) == 4:
            term, doc_id, term_freq, doc_length = parts
            term_freq = int(term_freq)
            doc_length = int(doc_length)
            
            # If we encounter a new term, process the previous term
            if current_term and current_term != term:
                doc_frequency = len(term_docs)
                term_data = {
                    "df": doc_frequency,
                    "docs": term_docs
                }
                print(f"{current_term}\t{json.dumps(term_data)}")
                
                term_docs = []
            
            current_term = term
            term_docs.append({"doc_id": doc_id, "tf": term_freq})
            
            doc_lengths[doc_id] = doc_length
            doc_terms[doc_id] += 1
            
            # Track of unique documents
            doc_count = len(doc_lengths)
            
    except Exception as e:
        sys.stderr.write(f"Error processing line: {line}, Error: {str(e)}\n")

if current_term:
    doc_frequency = len(term_docs)
    term_data = {
        "df": doc_frequency,
        "docs": term_docs
    }
    print(f"{current_term}\t{json.dumps(term_data)}")

# Output corpus statistics
corpus_stats = {
    "doc_count": doc_count,
    "avg_doc_length": sum(doc_lengths.values()) / doc_count if doc_count > 0 else 0,
    "doc_lengths": doc_lengths
}

print(f"__CORPUS_STATS__\t{json.dumps(corpus_stats)}")