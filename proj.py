import os

from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
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

def create_table():
    query = """
    CREATE TABLE public.subscribers_feedback (
        id serial4 NOT NULL,
        restaurant_id text NOT NULL,
        adv_campaign_id text NOT NULL,
        adv_campaign_content text NOT NULL,
        adv_campaign_owner text NOT NULL,
        adv_campaign_owner_contact text NOT NULL,
        adv_campaign_datetime_start int8 NOT NULL,
        adv_campaign_datetime_end int8 NOT NULL,
        datetime_created int8 NOT NULL,
        client_id text NOT NULL,
        trigger_datetime_created int4 NOT NULL,
        feedback varchar NULL,
        CONSTRAINT id_pk PRIMARY KEY (id)
    );
    """
    
    




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
        .where((col("adv_campaign_datetime_start") < current_timestamp_utc) & (col("adv_campaign_datetime_end") > current_timestamp_utc))

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

def join(df1: DataFrame, df2: DataFrame)-> DataFrame:
    return df1.crossJoin(df2)


def foreach_batch_function(df: DataFrame):
    df.persist()
    # сохраняем df в памяти, чтобы не создавать df заново перед отправкой в Kafka
    ...
    # записываем df в PostgreSQL с полем feedback
    ...
    # создаём df для отправки в Kafka. Сериализация в json.
    ...
    # отправляем сообщения в результирующий топик Kafka без поля feedback
    ...
    # очищаем память от df
    ...
    pass


def save_data(df):
    df.write \
        .mode("append") \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/de") \
        .option('driver', 'org.postgresql.Driver') \
        .option("dbtable", "subscribers_feedback") \
        .option("user", "jovyan") \
        .option("password", "jovyan") \
        .save()


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
result = join(df, stream_df)
save_data(result)

write_console(result)
