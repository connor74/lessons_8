import os

from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, to_json, col, lit, struct
from pyspark.sql.types import StructType, StructField, StringType, LongType

spark_jars_packages = ",".join(
        [
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
            "org.postgresql:postgresql:42.4.0",
        ]
    )
current_timestamp_utc = int(round(datetime.utcnow().timestamp()))
# создаём spark сессию с необходимыми библиотеками в spark_jars_packages для интеграции с Kafka и PostgreSQL



message_schema = StructType([
    StructField("restaurant_id", StringType()),
    StructField("adv_campaign_id", StringType()),
    StructField("adv_campaign_content", StringType()),
    StructField("adv_campaign_owner", StringType()),
    StructField("adv_campaign_owner_contact", StringType()),
    StructField("adv_campaign_datetime_start", LongType()),
    StructField("adv_campaign_datetime_end", LongType()),
    StructField("datetime_created", LongType()),
])




def spark_init() -> SparkSession:
    spark = SparkSession.builder \
        .appName("RestaurantSubscribeStreamingService") \
        .config("spark.sql.session.timeZone", "UTC") \
        .config("spark.jars.packages", spark_jars_packages) \
        .getOrCreate()
    return spark

def read_stream(spark: SparkSession) -> DataFrame:
    stream_df = spark.readStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091') \
        .option('kafka.security.protocol', 'SASL_SSL') \
        .option('kafka.sasl.jaas.config', 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"de-student\" password=\"ltcneltyn\";') \
        .option('kafka.sasl.mechanism', 'SCRAM-SHA-512') \
        .option("subscribe", 'kurzanovart_out') \
        .load()

    return stream_df \
        .select(from_json(col("value").cast("string"), message_schema).alias("parsed_key_value")) \
        .select(col("parsed_key_value.*")) \
        .where((col("adv_campaign_datetime_start") < current_timestamp_utc) & (col("adv_campaign_datetime_end") > current_timestamp_utc)) \

def read_data(spark: SparkSession) -> DataFrame:
    df = spark.read \
        .format('jdbc') \
        .option('url', 'jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de') \
        .option('driver', 'org.postgresql.Driver') \
        .option('dbtable', 'subscribers_restaurants') \
        .option('user', 'student') \
        .option('password', 'de-student') \
        .load()
    return df

def write_console(stream_df: DataFrame):
    query = (
    stream_df
        .writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", False)
        .start()
    )

    try:
        query.awaitTermination()
    finally:
        query.stop()

spark = spark_init()
stream_df = read_stream(spark)
df = read_data(spark)
df.show(10)
