#!/bin/bash
# Start ssh server
service ssh restart 

# Starting the services
bash start-services.sh

# Creating a virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install required packages
pip install -r requirements.txt  

# Package the virtual env.
venv-pack -o .venv.tar.gz

# Initialize Cassandra
python app.py

# Collect and prepare data
bash prepare_data.sh

# Run the indexer
bash index.sh /index/data

# Run sample searches
echo "Running sample searches..."
bash search.sh "death"