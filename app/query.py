import os
import numpy as np
from cassandra_driver import ConnectionHandler, DataFetcher

# Ranking algorithm constants
TERM_WEIGHT_FACTOR = 1.2
LENGTH_NORMALIZATION = 0.75


class SearchEngine:
    def __init__(self):
        self.db_handler = ConnectionHandler(
            hosts=['cassandra-server'], 
            keyspace='search_space'
        )
        self.data_processor = DataFetcher(self.db_handler)
        
    def _parse_search_phrase(self, input_source):
        """Extract and validate search terms from input"""
        if not input_source:
            raise ValueError("Empty input source")
            
        if isinstance(input_source, list):
            search_phrase = ' '.join(input_source[1:])
        else:
            search_phrase = input_source.strip()
            
        if not search_phrase:
            raise ValueError("No search terms provided")
            
        normalized_terms = search_phrase.lower().split()
        return normalized_terms, set(normalized_terms)
    
    def _compute_term_importance(self, doc_count, doc_freq):
        """Calculate inverse document frequency score"""
        return np.log((doc_count - doc_freq + 0.5) / (doc_freq + 0.5) + 1)
    
    def _calculate_document_score(self, freq, doc_len, avg_len, idf):
        """Compute BM25 ranking score for a single term-document pair"""
        numerator = freq * (TERM_WEIGHT_FACTOR + 1)
        denominator = freq + TERM_WEIGHT_FACTOR * (
            1 - LENGTH_NORMALIZATION + 
            LENGTH_NORMALIZATION * (doc_len / avg_len)
        )
        return idf * (numerator / denominator)
    
    def _fetch_corpus_metadata(self):
        """Retrieve statistical information about the document collection"""
        lengths = self.data_processor.get_document_lengths()
        if not lengths:
            return 0, 0.0
            
        total_docs = len(lengths)
        avg_length = sum(lengths) / total_docs
        return total_docs, avg_length
    
    def _process_term(self, term, doc_count, avg_len):
        """Handle scoring for a single search term"""
        freq_data = self.data_processor.get_term_frequency(term)
        if not freq_data:
            return {}
            
        doc_freq = freq_data['document_frequency']
        idf_score = self._compute_term_importance(doc_count, doc_freq)
        
        results = {}
        for entry in freq_data['postings']:
            doc_id = entry['document_id']
            tf = entry['term_frequency']
            doc_len = self.data_processor.get_doc_length(doc_id) or avg_len
            
            score = self._calculate_document_score(tf, doc_len, avg_len, idf_score)
            results[doc_id] = results.get(doc_id, 0.0) + score
            
        return results
    
    def _merge_scores(self, score_maps):
        """Combine scores from multiple terms"""
        combined = {}
        for score_map in score_maps:
            for doc_id, score in score_map.items():
                combined[doc_id] = combined.get(doc_id, 0.0) + score
        return combined
    
    def _format_output(self, ranked_docs, top_n=10):
        """Prepare results for display"""
        print("\n-----Top 10 Results:-----")
        for position, (doc_id, score) in enumerate(ranked_docs[:top_n], 1):
            # title = session.execute("SELECT title FROM doc_stats WHERE doc_id = %s", (doc_id, )).one()
            # print(title)
            print(f"{rank}. Document: {doc_id}   Score: {score:.3f}\n")
    
    def execute_search(self, input_source):
        """Main search execution flow"""
        try:
            terms, unique_terms = self._parse_search_phrase(input_source)
            print(f"Query: {' '.join(terms)}")
            
            doc_count, avg_len = self._fetch_corpus_metadata()
            
            all_scores = [
                self._process_term(term, doc_count, avg_len) 
                for term in unique_terms
            ]
            
            combined_scores = self._merge_scores(all_scores)
            sorted_results = sorted(
                combined_scores.items(), 
                key=lambda x: x[1], 
                reverse=True
            )
            
            self._format_output(sorted_results)
            
        except Exception as e:
            print(f"Search error: {str(e)}")
        finally:
            self.db_handler.close_connection()


def get_input_source():
    """Determine input source based on execution context"""
    if len(os.sys.argv) > 1:
        return os.sys.argv
    return os.sys.stdin.read()


if __name__ == "__main__":
    engine = SearchEngine()
    engine.execute_search(get_input_source())