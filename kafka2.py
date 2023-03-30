from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, TimestampType

# необходимая библиотека с идентификатором в maven
# вы можете использовать ее с помощью метода .config и опции "spark.jars.packages"
kafka_lib_id = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0"

# настройки security для кафки
# вы можете использовать из с помощью метода .options(**kafka_security_options)
kafka_security_options = {
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config': 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"de-student\" password=\"ltcneltyn\";',
}

def spark_init() -> SparkSession:
    spark_jars_packages = ",".join(
        [
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0"
        ]
    )
    spark = SparkSession.builder \
        .master("local") \
        .appName('test connect to kafka') \
        .config("spark.jars.packages", spark_jars_packages) \
        .getOrCreate()
    
    return spark



def load_df(spark: SparkSession) -> DataFrame:
    df = spark \
    .readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091') \
    .option('kafka.security.protocol', 'SASL_SSL') \
    .option('kafka.sasl.mechanism', 'SCRAM-SHA-512') \
    .option('kafka.sasl.jaas.config',
              'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"...\" password=\"...\";') \
    .option("subscribe", 'student.topic.cohort8.kurzanovart') \
    .load()

    return df



def transform(df: DataFrame) -> DataFrame:
    schema = StructType(
        [
            StructField("client_id", StringType()),
            StructField("timestamp", DoubleType()),
            StructField("lat", DoubleType()),
            StructField("lon", DoubleType()),
        ]
    )
 
    return df \
        .withColumn('value', f.col('value').cast(StringType())) \
        .withColumn('event', f.from_json(f.col('value'), schema)) \
        .selectExpr('event.*') \
        .withColumn('timestamp', f.from_unixtime(f.col('timestamp'), "yyyy-MM-dd' 'HH:mm:ss.SSS").cast(TimestampType()))
        
 


spark = spark_init()

source_df = load_df(spark)
output_df = transform(source_df)
output_df.printSchema()

query = (output_df
         .writeStream
         .outputMode("append")
         .format("console")
         .option("truncate", False)
         .trigger(once=True)
         .start())
try:
    query.awaitTermination()
finally:
    query.stop()


def transform(df: DataFrame) -> DataFrame:
    schema = StructType([
        StructField("client_id", StringType()),
        StructField("timestamp", DoubleType()),
        StructField("lat", DoubleType()),
        StructField("lon", DoubleType()),
    ])
 
    return (df
            .withColumn('value', f.col('value').cast(StringType()))
            .withColumn('event', f.from_json(f.col('value'), schema))
            .selectExpr('event.*')
            .withColumn('timestamp',
                        f.from_unixtime(f.col('timestamp'), "yyyy-MM-dd' 'HH:mm:ss.SSS").cast(TimestampType()))
            )
 
 

 
