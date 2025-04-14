from pathvalidate import sanitize_filename
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName('data preparation') \
    .master("local") \
    .config("spark.sql.parquet.enableVectorizedReader", "true") \
    .getOrCreate()

# Read the parquet file
df = spark.read.parquet("/a.parquet")
n = 1000
df = df.select(['id', 'title', 'text']).sample(fraction=100 * n / df.count(), seed=0).limit(n)

# Create documents in local filesystem
def create_doc(row):
    filename = "data/" + sanitize_filename(str(row['id']) + "_" + row['title']).replace(" ", "_") + ".txt"
    with open(filename, "w") as f:
        f.write(row['text'])

df.foreach(create_doc)

# Save data in HDFS for the MapReduce job
df.select("id", "title", "text").write.mode("overwrite").option("sep", "\t").csv("/index/data")