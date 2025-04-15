#!/usr/bin/env bash
# Hadoop MapReduce Indexing Pipeline
set -eo pipefail

# Constants
readonly HADOOP_STREAMING_JAR="${HADOOP_HOME}/share/hadoop/tools/lib/hadoop-streaming*.jar"
readonly MAPREDUCE_DIR="mapreduce"
readonly DEFAULT_INPUT_PATH="/data/sample.txt"
readonly INTERMEDIATE_OUTPUT="/index1/output1"
readonly FINAL_OUTPUT="/index/output"
readonly LOCAL_RESULT_FILE="v_data.txt"

# Logging
log() {
  local message="$1"
  echo "While indexing: ${message}"
}

# Checking and preparing the input data path
prepare_input_path() {
  local input_path="${1:-$DEFAULT_INPUT_PATH}"

  # Adding a slash if it does not exist
  [[ "$input_path" != /* ]] && input_path="/$input_path"

  echo "$input_path"
}

# Function to run Hadoop MapReduce
run_mapreduce_job() {
  local job_name="$1"
  local mapper="$2"
  local reducer="$3"
  local input="$4"
  local output="$5"

  log "Running MapReduce task: ${job_name}"

  hadoop jar ${HADOOP_STREAMING_JAR} \
    -D mapreduce.job.name="${job_name}" \
    -D mapreduce.job.reduces=1 \
    -files "${MAPREDUCE_DIR}/${mapper}.py,${MAPREDUCE_DIR}/${reducer}.py" \
    -mapper "python3 ${mapper}.py" \
    -reducer "python3 ${reducer}.py" \
    -input "${input}" \
    -output "${output}"
}

# Function for cleaning the HDFS output directory
cleanup_output_directory() {
  local output_path="$1"

  log "Deleting the previous output directory: ${output_path}"
  hdfs dfs -rm -r -f "${output_path}"
}

# Function for loading results to the local file system
retrieve_results() {
  local hdfs_path="$1"
  local local_path="$2"

  log "Downloading results from HDFS to a local file: ${local_path}"
  hdfs dfs -get -f "${hdfs_path}/part-00000" "${local_path}"
}

# Function for loading data into Cassandra
load_to_cassandra() {
  local index_file="$1"

  log "Loading the index in Cassandra"
  python3 app.py "${index_file}"
}

main() {
  local input_path=$(prepare_input_path "$1")
  cleanup_output_directory "${FINAL_OUTPUT}"
  cleanup_output_directory "${INTERMEDIATE_OUTPUT}"

  log "Starting the indexing process for: ${input_path}"

  # First job MapReduce
  run_mapreduce_job "BuildInvertedIndex_Stage1" "mapper1" "reducer1" \
    "${input_path}" "${INTERMEDIATE_OUTPUT}"

  # Second job MapReduce
  run_mapreduce_job "BuildInvertedIndex_Stage2" "mapper2" "reducer2" \
    "${INTERMEDIATE_OUTPUT}" "${FINAL_OUTPUT}"

  log "The MapReduce task has been completed. Results saved to HDFS: ${FINAL_OUTPUT}"

  retrieve_results "${FINAL_OUTPUT}" "${LOCAL_RESULT_FILE}"
  log "The indexing results are saved to a local file: ${LOCAL_RESULT_FILE}"

  load_to_cassandra "${LOCAL_RESULT_FILE}"

  log "The indexing process has been successfully completed. The inverted index and statistics are uploaded to Cassandra."
}

# Start the main function

main "$1"