import sys
import json
from cassandra.cluster import Cluster

# Initialize Cassandra connection
cluster = Cluster(['cassandra-server'])
session = cluster.connect()

# Create keyspace if not exists
session.execute("""
    CREATE KEYSPACE IF NOT EXISTS search_space
    WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }
""")

# Use the keyspace
session.execute("USE search_space")

# Create tables if they don't exist
session.execute("""
    CREATE TABLE IF NOT EXISTS vocabulary (
        term text PRIMARY KEY,
        doc_frequency int
    )
""")

session.execute("""
    CREATE TABLE IF NOT EXISTS postings (
        term text,
        doc_id text,
        term_frequency int,
        PRIMARY KEY (term, doc_id)
    )
""")

session.execute("""
    CREATE TABLE IF NOT EXISTS corpus_stats (
        id text PRIMARY KEY,
        doc_count int,
        avg_doc_length float
    )
""")

session.execute("""
    CREATE TABLE IF NOT EXISTS document_stats (
        doc_id text PRIMARY KEY,
        doc_length int
    )
""")

vocab_insert = session.prepare("INSERT INTO vocabulary (term, doc_frequency) VALUES (?, ?)")
posting_insert = session.prepare("INSERT INTO postings (term, doc_id, term_frequency) VALUES (?, ?, ?)")
doc_stats_insert = session.prepare("INSERT INTO document_stats (doc_id, doc_length) VALUES (?, ?)")
corpus_stats_insert = session.prepare("INSERT INTO corpus_stats (id, doc_count, avg_doc_length) VALUES (?, ?, ?)")

# Process the output from the second mapper
vocab_batch = []
posting_batch = []
doc_stats_batch = []

for line in sys.stdin:
    try:
        parts = line.strip().split('\t')
        record_type = parts[0]
        
        if record_type == "VOCAB":
            term, doc_freq = parts[1], int(parts[2])
            vocab_batch.append((term, doc_freq))
            
            if len(vocab_batch) >= 100:
                for item in vocab_batch:
                    session.execute(vocab_insert, item)
                vocab_batch = []
                
        elif record_type == "POSTING":
            term, doc_id, term_freq = parts[1], parts[2], int(parts[3])
            posting_batch.append((term, doc_id, term_freq))
            
            if len(posting_batch) >= 100:
                for item in posting_batch:
                    session.execute(posting_insert, item)
                posting_batch = []
                
        elif record_type == "__CORPUS_STATS__":
            corpus_data = json.loads(parts[1])
            doc_count = corpus_data["doc_count"]
            avg_doc_length = corpus_data["avg_doc_length"]
            
            session.execute(corpus_stats_insert, ("global", doc_count, avg_doc_length))
            
            for doc_id, doc_length in corpus_data["doc_lengths"].items():
                doc_stats_batch.append((doc_id, doc_length))
                
            if len(doc_stats_batch) >= 100:
                for item in doc_stats_batch:
                    session.execute(doc_stats_insert, item)
                doc_stats_batch = []
                
    except Exception as e:
        sys.stderr.write(f"Error processing line: {line}, Error: {str(e)}\n")

# Insert any remaining batch items
for item in vocab_batch:
    session.execute(vocab_insert, item)
    
for item in posting_batch:
    session.execute(posting_insert, item)
    
for item in doc_stats_batch:
    session.execute(doc_stats_insert, item)

# Close the connection
cluster.shutdown()