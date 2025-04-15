#!/bin/bash
# Start ssh server
service ssh restart 

echo "Starting the services..."
bash start-services.sh

echo "Creating a virtual environment..."
python3 -m venv .venv
source .venv/bin/activate

echo "Install packages..."
pip install -r requirements.txt  

echo "Package the virtual env..."
venv-pack -o .venv.tar.gz

echo "Collect data..."
bash prepare_data.sh

echo "Run the indexer..."
bash index.sh data/sample.txt

# Run sample searches
echo "Running sample searches..."
bash search.sh "death"
bash search.sh "Economist book"
bash search.sh "Hero of our time"
bash search.sh "crime fiction"