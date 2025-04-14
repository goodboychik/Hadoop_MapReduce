#!/bin/bash
echo "Running MapReduce jobs to index documents"

# Activate virtual environment
source .venv/bin/activate

# Get the input path
INPUT_PATH=$1
if [ -z "$INPUT_PATH" ]; then
    INPUT_PATH="/index/data"
    echo "Using default input path: $INPUT_PATH"
else
    echo "Using input path: $INPUT_PATH"
    
    # If input is from local file system, copy to HDFS
    if [[ "$INPUT_PATH" != //* ]]; then
        hdfs dfs -put -f "$INPUT_PATH" /tmp/index_input
        INPUT_PATH="/tmp/index_input"
    fi
fi

# Create output directories
hdfs dfs -rm -r -f /tmp/index_output1
hdfs dfs -rm -r -f /tmp/index_output2

# Make mapper and reducer executable
chmod +x mapreduce/mapper1.py
chmod +x mapreduce/reducer1.py
chmod +x mapreduce/mapper2.py
chmod +x mapreduce/reducer2.py

# First MapReduce job - Extract terms and term frequencies
echo "Starting first MapReduce job..."
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -files mapreduce/mapper1.py,mapreduce/reducer1.py \
    -mapper mapreduce/mapper1.py \
    -reducer mapreduce/reducer1.py \
    -input $INPUT_PATH \
    -output /tmp/index_output1

echo "First MapReduce job completed."

# Second MapReduce job - Store data in Cassandra
echo "Starting second MapReduce job..."
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -files mapreduce/mapper2.py,mapreduce/reducer2.py \
    -mapper mapreduce/mapper2.py \
    -reducer mapreduce/reducer2.py \
    -input /tmp/index_output1 \
    -output /tmp/index_output2

echo "Second MapReduce job completed."
echo "Indexing completed successfully!"