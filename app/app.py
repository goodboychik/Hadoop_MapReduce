from cassandra.cluster import Cluster

def setup_cassandra():
    print("Setting up Cassandra database...")
    
    # Connect to Cassandra
    cluster = Cluster(['cassandra-server'])
    session = cluster.connect()
    
    # Create keyspace if it doesn't exist
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
    
    # List all tables in the keyspace
    rows = session.execute("SELECT table_name FROM system_schema.tables WHERE keyspace_name = 'search_space'")
    print("Tables in search_engine keyspace:")
    for row in rows:
        print(f"- {row.table_name}")
    
    # Close the connection
    cluster.shutdown()
    print("Cassandra setup completed successfully!")

if __name__ == "__main__":
    setup_cassandra()