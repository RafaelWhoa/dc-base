from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os

load_dotenv()

spark = (
    SparkSession.builder
    .appName("KafkaToS3")
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY"))
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET"))
    .config("spark.hadoop.fs.s3a.connection.timeout", "60000")  # sem "s"
    .getOrCreate()
)

print("Spark conectado:", spark.version)

# Configuração MinIO
hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.endpoint", "http://localhost:9000")
hadoop_conf.set("fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY"))
hadoop_conf.set("fs.s3a.secret.key", os.getenv("MINIO_SECRET"))
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
hadoop_conf.set("fs.s3a.connection.timeout", "60000")   # 60s
hadoop_conf.set("fs.s3a.connection.establish.timeout", "60000")
hadoop_conf.set("fs.s3a.attempts.maximum", "3")
hadoop_conf.set("fs.s3a.connection.maximum", "100")

# Ler do Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:29092") \
    .option("subscribe", "topico-teste") \
    .load()

logs = df.selectExpr("CAST(value AS STRING) as json_str")

# Escrever no MinIO
query = logs.writeStream \
    .format("parquet") \
    .option("path", "s3a://fakelogs/logs/") \
    .option("checkpointLocation", "s3a://fakelogs/checkpoints/") \
    .outputMode("append") \
    .start()

query.awaitTermination()