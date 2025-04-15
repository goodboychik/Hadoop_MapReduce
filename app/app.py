import os
import re
from cassandra.cluster import Cluster
from dataclasses import dataclass
from typing import Dict, List, Optional


@dataclass
class DatabaseConfig:
    hosts: List[str] = None
    keyspace: str = "text_search_db"
    replication_config: Dict = None

    def __post_init__(self):
        self.hosts = self.hosts or ["cassandra-node"]
        self.replication_config = self.replication_config or {
            'class': 'SimpleStrategy',
            'replication_factor': 1
        }


class CassandraStorageManager:
    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.cluster = None
        self.session = None

    def __enter__(self):
        self.establish_connection()
        self.prepare_schema()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.cluster:
            self.cluster.shutdown()

    def establish_connection(self):
        print(f"Initializing connection to Cassandra at {self.config.hosts}")
        self.cluster = Cluster(self.config.hosts)
        self.session = self.cluster.connect()
        
        keyspace_query = f"""
            CREATE KEYSPACE IF NOT EXISTS {self.config.keyspace}
            WITH REPLICATION = {self._format_replication_config()}
        """
        self.session.execute(keyspace_query)
        self.session.set_keyspace(self.config.keyspace)
        print(f"Using keyspace: {self.config.keyspace}")

    def _format_replication_config(self) -> str:
        items = [f"'{k}': '{v}'" for k, v in self.config.replication_config.items()]
        return "{" + ", ".join(items) + "}"

    def prepare_schema(self):
        schema_definitions = {
            'document_metadata': """
                CREATE TABLE IF NOT EXISTS document_metadata (
                    doc_id text PRIMARY KEY,
                    title text,
                    length int
                )
            """,
            'corpus_metrics': """
                CREATE TABLE IF NOT EXISTS corpus_metrics (
                    metric_id text PRIMARY KEY,
                    document_count int,
                    avg_length float
                )
            """,
            'term_index': """
                CREATE TABLE IF NOT EXISTS term_index (
                    term text,
                    doc_id text,
                    frequency int,
                    PRIMARY KEY (term, doc_id)
                )
            """,
            'term_stats': """
                CREATE TABLE IF NOT EXISTS term_stats (
                    term text PRIMARY KEY,
                    doc_count int
                )
            """
        }

        print("Configuring database schema...")
        for table, definition in schema_definitions.items():
            self.session.execute(definition)
            print(f" - Table '{table}' ready")


class TextDataProcessor:
    @staticmethod
    def extract_metadata(source_path: str) -> tuple[Dict[str, str], Dict[str, int]]:
        titles = {}
        lengths = {}
        
        if not os.path.exists(source_path):
            print(f"Metadata file not found at {source_path}")
            return titles, lengths

        print(f"Processing metadata from {source_path}")
        with open(source_path, encoding='utf-8') as f:
            for record in f:
                if not record.strip():
                    continue
                
                parts = record.rstrip().split('\t')
                if len(parts) < 3:
                    continue
                
                doc_id, title, content = parts[0], parts[1], parts[2]
                titles[doc_id] = title
                lengths[doc_id] = len([
                    word for word in re.findall(r'\w+', content.lower())
                    if word and not word.isdigit()
                ])

        print(f"Processed {len(titles)} document records")
        return titles, lengths

    @staticmethod
    def process_index_file(file_path: str, storage: CassandraStorageManager, 
                         titles: Dict[str, str], lengths: Dict[str, int]):
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Index data file missing: {file_path}")

        print(f"Importing index data from {file_path}")
        with open(file_path, encoding='utf-8') as f:
            for entry in f:
                if not entry.strip():
                    continue
                
                fields = entry.split('\t')
                if fields[0] == "VOCAB" and len(fields) >= 3:
                    storage.session.execute(
                        "INSERT INTO term_stats (term, doc_count) VALUES (%s, %s)",
                        (fields[1], int(fields[2]))
                    )
                elif fields[0] == "POST" and len(fields) >= 4:
                    term, doc_id = fields[1], fields[2]
                    count = int(fields[3]) if fields[3].isdigit() else 0
                    
                    storage.session.execute(
                        "INSERT INTO term_index (term, doc_id, frequency) VALUES (%s, %s, %s)",
                        (term, doc_id, count)
                    )
                    
                    if doc_id not in lengths:
                        lengths[doc_id] = 0
                    lengths[doc_id] += count
                    
                    if doc_id not in titles:
                        titles[doc_id] = ""


class DataLoader:
    @staticmethod
    def save_document_metrics(storage: CassandraStorageManager, 
                            titles: Dict[str, str], lengths: Dict[str, int]):
        print(f"Persisting metrics for {len(lengths)} documents")
        for doc_id, length in lengths.items():
            storage.session.execute(
                "INSERT INTO document_metadata (doc_id, title, length) VALUES (%s, %s, %s)",
                (doc_id, titles.get(doc_id, ""), length)
        print("Document metrics stored successfully")


def execute_data_pipeline(index_file_path: str = "output/index_results.txt"):
    db_config = DatabaseConfig()
    data_source = "resources/text_samples.tsv"

    with CassandraStorageManager(db_config) as db:
        doc_titles, doc_lengths = TextDataProcessor.extract_metadata(data_source)
        TextDataProcessor.process_index_file(index_file_path, db, doc_titles, doc_lengths)
        DataLoader.save_document_metrics(db, doc_titles, doc_lengths)
        print("Data pipeline completed successfully")


if __name__ == "__main__":
    import sys
    input_path = sys.argv[1] if len(sys.argv) > 1 else "output/index_results.txt"
    execute_data_pipeline(input_path)