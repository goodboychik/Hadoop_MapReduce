import sys
import json

# Process the output from the first reducer
for line in sys.stdin:
    try:
        parts = line.strip().split('\t', 1)
        if len(parts) == 2:
            term, data_json = parts
            
            if term == "__CORPUS_STATS__":
                print(f"{term}\t{data_json}")
            else:
                data = json.loads(data_json)
                doc_freq = data["df"]
                
                print(f"VOCAB\t{term}\t{doc_freq}")
                
                for doc_entry in data["docs"]:
                    doc_id = doc_entry["doc_id"]
                    term_freq = doc_entry["tf"]
                    print(f"POSTING\t{term}\t{doc_id}\t{term_freq}")
    except Exception as e:
        sys.stderr.write(f"Error processing line: {line}, Error: {str(e)}\n")