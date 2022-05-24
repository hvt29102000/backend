from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark import SparkContext
from utils import getOrCreateSparkSession
from import_mongo import writeDfToMongo, readFromMongo, writeToMongo
from pyspark.sql.types import ArrayType, StructField, StructType, StringType, IntegerType, DecimalType
from CFAlgo import CF
TOPIC_NAME = "movies"
OUTPUT_TOPIC_NAME = "result"
BOOTSTRAP_SERVERS = 'localhost:9092'


spark = getOrCreateSparkSession("full")
spark.sparkContext.setLogLevel("ERROR")


#  read new data
rowdf = spark.readStream.format("kafka").option(
    "kafka.bootstrap.servers", BOOTSTRAP_SERVERS)\
    .option("subscribe", TOPIC_NAME)\
    .load()

schema = StructType([
    StructField('movieId', IntegerType(), True),
    StructField('rating', IntegerType(), True),
    StructField('userId', StringType(), True),
])
# if "value" in rowdf.columns:
# df = rowdf.select(from_json(rowdf["value"].cast("string"), schema))
df = rowdf.select(from_json(rowdf.value.cast(
    "string"), schema).alias("document"))


# writeDfToMongo(spark, rowdf, "ratings_copy")


def processRecommendation(df, epoch_id):
    print("---------------writing to mongo-----------------")
    writeDfToMongo(df, epoch_id)
    #  read from db and process

    df = readFromMongo("ratings_copy", spark)
    df = df.drop("_id")
    df = df.drop("timestamp")
    df = df[['userId', 'movieId', 'rating']]
    df.withColumn("movieId", df.movieId.cast('string'))
    cf = CF(spark, df)
    print("----------------processing-------------------")
    result = cf.processRecommendations()
    writeToMongo(spark, result, "recommendation")
    checkpoint_path = "/mnt/d/UbuntuWSL2/checkpoint"
    query = rowdf.writeStream.format("kafka")\
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)\
        .option("topic", "result")\
        .option("checkpointLocation", checkpoint_path)\
        .start()


queryMongo = df.writeStream.foreachBatch(processRecommendation).start()
queryMongo.awaitTermination()

# rs = processRecommendation()
# print(rs)


# query = df.writeStream\
#     .format("console")\
#     .start()
# query = df.writeStream.outputMode("append").format("console").start()

# queryMongo.awaitTermination()


# lines.printSchema()
