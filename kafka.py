import os
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, TimestampType, IntegerType

postgresql_settings = {
    'user': '--- hello ---',
    'password': '--- hello ---'
}

kafka_security_options = {
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config': 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"--- hello ---\" password=\"--- hello ---\";',
}

incomming_message_schema = StructType([
    StructField("restaurant_id", StringType(), True),
    StructField("adv_campaign_id", StringType(), True),
    StructField("adv_campaign_content", StringType(), True),
    StructField("adv_campaign_owner", StringType(), True),
    StructField("adv_campaign_owner_contact", StringType(), True),
    StructField("adv_campaign_datetime_start", TimestampType(), True),
    StructField("adv_campaign_datetime_end", TimestampType(), True),
    StructField("datetime_created", TimestampType(), True)])

current_timestamp_utc = datetime.utcnow()

spark = SparkSession.builder \
    .appName("RestaurantSubscribeStreamingService") \
    .config("spark.sql.session.timeZone", "UTC") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0," + "org.postgresql:postgresql:42.4.0") \
    .getOrCreate()

def foreach_batch_function(df, epoch_id):
    rslt = df.persist()

    rslt.drop('id') \
	.write \
        .format('jdbc') \
        .mode('append') \
        .option('url', 'jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de') \
        .option('driver', 'org.postgresql.Driver') \
        .option('dbtable', ' public.subscribers_feedback') \
        .option(**postgresql_settings) \
        .save()

    kafka_out_df = rslt.withColumn('value', f.to_json(f.struct('restaurant_id', 'adv_campaign_id', 'adv_campaign_content', 'adv_campaign_owner', 'adv_campaign_owner_contact', 'adv_campaign_datetime_start', 'adv_campaign_datetime_end', 'client_id', 'datetime_created', 'trigger_datetime_created'))) \
        .select('value')

    kafka_out_df \
        .writeStream \
        .outputMode("append") \
        .format('kafka') \
        .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091') \
        .option(**kafka_security_options) \
        .trigger(continuous='1 second') \
        .option("truncate", False) \
        .option('topic', 'kafka_write') \
        .start()
    
    rslt.unpersist()

restaurant_read_stream_df = spark.readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091') \
    .options(**kafka_security_options) \
    .option('subscribe', 'kafka_read') \
    .option("startingOffsets", "earliest") \
    .load()

filtered_read_stream_df = restaurant_read_stream_df \
	.withColumn('value', f.col('value').cast(StringType())) \
	.withColumn('value', f.from_json('value', incomming_message_schema)) \
	.select(f.col('value.*')) \
	.dropDuplicates() \
	.withWatermark('datetime_created', '10 minute') \
	.filter((f.col('adv_campaign_datetime_start') <= current_timestamp_utc) & (
            f.col('adv_campaign_datetime_end') >= current_timestamp_utc))

subscribers_restaurant_df = spark.read \
    .format('jdbc') \
    .option('url', 'jdbc:postgresql://--- hello ---') \
    .option('driver', 'org.postgresql.Driver') \
    .option('dbtable', '--- hello ---') \
    .options(**postgresql_settings) \
   .load()

result_df = filtered_read_stream_df.join(subscribers_restaurant_df, 'restaurant_id', 'inner') \
    .withColumn('trigger_datetime_created', f.current_timestamp()) \
    .withColumn('feedback', f.lit(None).cast(StringType()))

result_df.writeStream \
    .foreachBatch(foreach_batch_function) \
    .start() \
    .awaitTermination()
