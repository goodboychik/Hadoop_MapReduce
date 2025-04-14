import sys
import re
import nltk
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer

# Download NLTK resources
nltk.download('stopwords', quiet=True)

# Initialize stemmer and stopwords
stemmer = PorterStemmer()
stop_words = set(stopwords.words('english'))

# Function to tokenize and process text
def tokenize(text):
    # Convert to lowercase and split into tokens
    tokens = re.findall(r'\b\w+\b', text.lower())
    # Remove stopwords and apply stemming
    tokens = [stemmer.stem(token) for token in tokens if token not in stop_words and len(token) > 1]
    return tokens

# Process input lines from stdin
for line in sys.stdin:
    try:
        parts = line.strip().split('\t')
        if len(parts) >= 3:
            doc_id, doc_title, doc_text = parts[0], parts[1], parts[2]
            
            tokens = tokenize(doc_text)
            
            term_freq = {}
            for token in tokens:
                if token in term_freq:
                    term_freq[token] += 1
                else:
                    term_freq[token] = 1
            
            doc_length = len(tokens)
            for term, freq in term_freq.items():
                print(f"{term}\t{doc_id}\t{freq}\t{doc_length}")
    except Exception as e:
        sys.stderr.write(f"Error processing line: {line}, Error: {str(e)}\n")