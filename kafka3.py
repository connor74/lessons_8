#!/usr/bin/env python
# coding: utf-8


from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, TimestampType, IntegerType




spark_jars_packages = ",".join(
        [
            "org.postgresql:postgresql:42.4.0",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0"
        ]
    )
def spark_init(test_name) -> SparkSession:
	  return (SparkSession
        .builder
        .master("local")
        .config("spark.jars.packages", spark_jars_packages)
        .appName(f"{test_name}")
        .getOrCreate()
            )




postgresql_settings = {
    'user': 'student',
    'password': 'de-student'
}


def read_marketing(spark: SparkSession) -> DataFrame:
    return spark.read \
                    .format('jdbc') \
                    .option('url', 'jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de') \
                    .option('driver', 'org.postgresql.Driver') \
                    .option('schema','public') \
                    .option('dbtable', 'marketing_companies') \
                    .option('user', postgresql_settings["user"]) \
                    .option('password', postgresql_settings["password"]) \
                    .load()


schema = StructType([
    StructField("client_id", StringType()),
    StructField("timestamp", DoubleType()),
    StructField("lat", DoubleType()),
    StructField("lon", DoubleType()),
])

def read_client_stream(spark: SparkSession) -> DataFrame:
    df = (spark.readStream.format('kafka')
          .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091')
          .option('kafka.security.protocol', 'SASL_SSL')
          .option('kafka.sasl.mechanism', 'SCRAM-SHA-512')
          .option('kafka.sasl.jaas.config',
                  'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"de-student\" password=\"ltcneltyn\";') \
          .option("subscribe", "student.topic.cohort8.kurzanovart")
          .load()
          .withColumn('value', f.col('value').cast(StringType()))
          .withColumn('event', f.from_json(f.col('value'), schema))
          .selectExpr('event.*')
          .withColumn('timestamp',
                      f.from_unixtime(f.col('timestamp'), "yyyy-MM-dd' 'HH:mm:ss.SSS").cast(TimestampType()))
          .dropDuplicates(['client_id', 'timestamp'])
          .withWatermark('timestamp', '10 minutes')
          )
    return df





def join(user_df, marketing_df) -> DataFrame:
    df = marketing_df.crossJoin(user_df)
    
    return (df            
            .withColumn("a", 
                        ( f.pow(f.sin(f.radians(marketing_df.point_lat - user_df.lat) / 2), 2) + f.cos(f.radians(user_df.lat)) 
                         * f.cos(f.radians(marketing_df.point_lat)) 
                         * f.pow(f.sin(f.radians(marketing_df.point_lon - user_df.lon) / 2), 2))
                       ) 
            .withColumn("d", (f.atan2(f.sqrt(f.col("a")), f.sqrt(1 - f.col("a"))) * 2 * 6371000 )) 
            .withColumn("distance", f.col('d').cast(IntegerType()))
            .where(f.col("distance") <= 1000)
            .withColumn("adv_campaign_id", marketing_df.id)
            .withColumn("adv_campaign_name", marketing_df.name)
            .withColumn("adv_campaign_description", marketing_df.description)
            .withColumn("adv_campaign_start_time", marketing_df.start_time)
            .withColumn("adv_campaign_end_time", marketing_df.end_time)
            .withColumn("adv_campaign_point_lat", marketing_df.point_lat)
            .withColumn("adv_campaign_point_lon", marketing_df.point_lon)
            .withColumn("client_id", f.substring('client_id', 0, 6))
            .withColumn("created_at", f.lit(datetime.now()))
            .dropDuplicates(['client_id', 'adv_campaign_id'])
            .withWatermark('timestamp', '1 minutes')
            .select('client_id',
                    'distance',
                    'adv_campaign_id',
                    'adv_campaign_name',
                    'adv_campaign_description',
                    'adv_campaign_start_time',
                    'adv_campaign_end_time',
                    "adv_campaign_point_lat",
                    "adv_campaign_point_lon",
                    'created_at')
           )




spark = spark_init('join stream')
client_stream = read_client_stream(spark)
marketing_df = read_marketing(spark)
result = join(client_stream, marketing_df)

query = (result
         .writeStream
         .outputMode("append")
         .format("console")
         .option("truncate", False)
         .start())
query.awaitTermination()
