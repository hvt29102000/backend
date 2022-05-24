from pyspark.sql import SparkSession
from import_mongo import readFromMongo
# import findspark
# findspark.init(
#     "/mnt/d/UbuntuWSL2/spark-3.1.2-bin-hadoop3.2/spark-3.1.2-bin-hadoop3.2")
TOPIC_NAME = "movies"
OUTPUT_TOPIC_NAME = "result"
BOOTSTRAP_SERVERS = 'localhost:9092'


def getOrCreateSparkSession(type):
    spark = None
    if type == "kafka":
        spark = SparkSession \
            .builder \
            .appName("PySpark Structured Streaming with Kafka Demo") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2")\
            .getOrCreate()
    if type == "mongo":
        spark = SparkSession.builder.appName("SimpleApp")\
            .config("spark.mongodb.input.uri", "mongodb+srv://carie_admin:carie.admin@cluster0.fteep.mongodb.net/movielens.movies")\
            .config("spark.mongodb.output.uri", "mongodb+srv://carie_admin:carie.admin@cluster0.fteep.mongodb.net/movielens.movies")\
            .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.0")\
            .getOrCreate()
    if type == "full":
        spark = SparkSession.builder.appName("SimpleAppFull")\
            .config("spark.mongodb.input.uri", "mongodb+srv://carie_admin:carie.admin@cluster0.fteep.mongodb.net/movielens.movies")\
            .config("spark.mongodb.output.uri", "mongodb+srv://carie_admin:carie.admin@cluster0.fteep.mongodb.net/movielens.movies")\
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,org.mongodb.spark:mongo-spark-connector_2.12:3.0.0")\
            .getOrCreate()
    return spark


# spark = getOrCreateSparkSession("mongo")
# readFromMongo("ratings_copy", spark)
# spark.sparkContext.setLogLevel("ERROR")

# df = spark.readStream.format("kafka").option(
#     "kafka.bootstrap.servers", BOOTSTRAP_SERVERS)\
#     .option("subscribe", TOPIC_NAME)\
#     .load()
# # df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# checkpoint_path = "/mnt/d/UbuntuWSL2/checkpoint"
# query = df.writeStream.format("kafka")\
#     .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)\
#     .option("topic", "result")\
#     .option("checkpointLocation", checkpoint_path)\
#     .start()
# query.awaitTermination()

# query = df.writeStream.outputMode("append").format("console").start()
# query.awaitTermination()
