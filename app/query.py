import sys
import re
import nltk
import math
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer
from pyspark import SparkContext
from cassandra.cluster import Cluster

nltk.download('stopwords', quiet=True)

K1 = 1.2
B = 0.75

def connect_to_cassandra():
    """Connect to Cassandra and return a session"""
    cluster = Cluster(['cassandra-server'])
    session = cluster.connect('search_engine')
    return session, cluster

def tokenize_query(query):
    """Tokenize and process the query text"""
    stemmer = PorterStemmer()
    stop_words = set(stopwords.words('english'))
    
    # Convert to lowercase and split into tokens
    tokens = re.findall(r'\b\w+\b', query.lower())
    # Remove stopwords and apply stemming
    tokens = [stemmer.stem(token) for token in tokens if token not in stop_words and len(token) > 1]
    return tokens

def get_corpus_stats(session):
    """Get corpus statistics from Cassandra"""
    row = session.execute("SELECT doc_count, avg_doc_length FROM corpus_stats WHERE id = 'global'").one()
    return row.doc_count, row.avg_doc_length

def get_document_length(session, doc_id):
    """Get document length from Cassandra"""
    row = session.execute("SELECT doc_length FROM document_stats WHERE doc_id = %s", [doc_id]).one()
    return row.doc_length if row else 0

def get_term_doc_freq(session, term):
    """Get document frequency for a term"""
    row = session.execute("SELECT doc_frequency FROM vocabulary WHERE term = %s", [term]).one()
    return row.doc_frequency if row else 0

def get_term_postings(session, term):
    """Get postings for a term"""
    rows = session.execute("SELECT doc_id, term_frequency FROM postings WHERE term = %s", [term])
    return {row.doc_id: row.term_frequency for row in rows}

def calculate_bm25(tf, df, doc_length, avg_doc_length, doc_count):
    """Calculate BM25 score for a term in a document"""
    idf = math.log((doc_count - df + 0.5) / (df + 0.5) + 1)
    numerator = tf * (K1 + 1)
    denominator = tf + K1 * (1 - B + B * (doc_length / avg_doc_length))
    return idf * (numerator / denominator)

def main(query):
    """Main function to process the query and retrieve documents"""
    sc = SparkContext(appName="SearchEngine")
    
    session, cluster = connect_to_cassandra()
    
    try:
        query_terms = tokenize_query(query)
        
        if not query_terms:
            print("No valid search terms in the query.")
            return
        
        doc_count, avg_doc_length = get_corpus_stats(session)
        
        all_doc_scores = {}
        
        for term in query_terms:
            df = get_term_doc_freq(session, term)
            
            if df > 0:
                postings = get_term_postings(session, term)
                
                for doc_id, tf in postings.items():
                    doc_length = get_document_length(session, doc_id)
                    
                    if doc_length > 0:
                        score = calculate_bm25(tf, df, doc_length, avg_doc_length, doc_count)
                        
                        if doc_id in all_doc_scores:
                            all_doc_scores[doc_id] += score
                        else:
                            all_doc_scores[doc_id] = score
        
        doc_scores_rdd = sc.parallelize(all_doc_scores.items())
        sorted_docs = doc_scores_rdd.sortBy(lambda x: -x[1]).take(10)
        
        print("\nTop 10 documents for query:", query)
        print("-----------------------------")
        if sorted_docs:
            for i, (doc_id, score) in enumerate(sorted_docs, 1):
                doc_parts = doc_id.split('_', 1)
                doc_title = doc_parts[1].replace('_', ' ') if len(doc_parts) > 1 else doc_id
                print(f"{i}. Document ID: {doc_id}")
                print(f"   Title: {doc_title}")
                print(f"   BM25 Score: {score:.4f}")
                print()
        else:
            print("No matching documents found.")
            
    finally:
        cluster.shutdown()
        sc.stop()

if __name__ == "__main__":
    if len(sys.argv) > 1:
        query = ' '.join(sys.argv[1:])
        main(query)
    else:
        print("Please provide a search query.")