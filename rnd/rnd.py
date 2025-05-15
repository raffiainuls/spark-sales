from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StringType, StructType, StructField, IntegerType
from pyspark.sql import DataFrame

# 1. Buat SparkSession
spark = SparkSession.builder \
    .appName("KafkaSparkStreamingETL") \
    .master("local[2]") \
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,"
            "org.postgresql:postgresql:42.5.4") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .getOrCreate()

# 2. Matikan pemanggilan native Windows API untuk Hadoop (biar gak error nativeio)
spark.sparkContext._jsc.hadoopConfiguration().set("hadoop.native.lib", "false")

# 3. Baca dari Kafka
territory_kafka_stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9093") \
    .option("subscribe", "server.public.territory") \
    .option("startingOffsets", "earliest") \
    .load()
region_kafka_stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9093") \
    .option("subscribe", "server.public.region") \
    .option("startingOffsets", "earliest") \
    .load()



# 4. Ambil kolom value & convert ke string
territory_df = territory_kafka_stream_df.selectExpr("CAST(value AS STRING)")
region_df = region_kafka_stream_df.selectExpr("CAST(value AS STRING)")


# 5. Schema yang sesuai struktur Kafka JSON (dengan schema & payload)
territory_kafka_schema = StructType([
    StructField("schema", StructType([])),  # Optional, kita abaikan
    StructField("payload", StructType([
        StructField("territoryid", IntegerType(), False),
        StructField("territorydescription", StringType(), True),
        StructField("regionid", IntegerType(), True)
    ]))
])
region_kafka_schema = StructType([
    StructField("schema", StructType([])),  # Optional, kita abaikan
    StructField("payload", StructType([
        StructField("regionid", IntegerType(), False),
        StructField("regiondescription", StringType(), True)
    ]))
])


# 6. Parse JSON dari Kafka messages menjadi DataFrame
territory_parsed_df = territory_df \
    .filter(col('value').contains('"territoryid"')) \
    .select(from_json(col('value'), territory_kafka_schema).alias('json')) \
    .select("json.payload.*")
region_parsed_df = region_df \
    .filter(col('value').contains('"regionid"')) \
    .select(from_json(col('value'), region_kafka_schema).alias('json')) \
    .select("json.payload.*")

# 7. Register sebagai temporary view
territory_parsed_df.createOrReplaceTempView("territory")
region_parsed_df.createOrReplaceTempView("region")

# 8. Query ETL pakai Spark SQL
etl_query_df = spark.sql("""
    SELECT 
        t.territoryid,
        t.territorydescription,
        t.regionid,
        r.regiondescription
    FROM territory t
    JOIN region r
    ON t.regionid = r.regionid
""")

# # 9. Sink ke PostgreSQL pakai foreachBatch
# def write_to_postgres(batch_df: DataFrame, batch_id: int):
#     batch_df.write \
#         .format("jdbc") \
#         .option("url", "jdbc:postgresql://localhost:5432/postgres") \
#         .option("dbtable", "public.postgres_table") \
#         .option("user", "postgres") \
#         .option("password", "postgres") \
#         .option("driver", "org.postgresql.Driver") \
#         .mode("append") \
#         .save()

# query = etl_query_df.writeStream \
#     .foreachBatch(write_to_postgres) \
#     .outputMode("append") \
#     .option("checkpointLocation", "/tmp/spark-checkpoint/territory-region") \
#     .start()

# query.awaitTermination()


# print("----------- SESSION DEBUGGING ----------------")

#5. Tampilkan ke console
query2 = region_parsed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query2.awaitTermination()





