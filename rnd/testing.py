from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StringType, StructType, StructField, IntegerType

# 1. SparkSession
spark = SparkSession.builder \
    .appName("KafkaSparkStreamingETL") \
    .master("local[2]") \
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,"
            "org.postgresql:postgresql:42.5.4") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .getOrCreate()

spark.sparkContext._jsc.hadoopConfiguration().set("hadoop.native.lib", "false")

# 2. Baca stream Kafka
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

# 3. Parsing value
territory_df = territory_kafka_stream_df.selectExpr("CAST(value AS STRING)")
region_df = region_kafka_stream_df.selectExpr("CAST(value AS STRING)")

# 4. Schema
territory_kafka_schema = StructType([
    StructField("schema", StructType([])),
    StructField("payload", StructType([
        StructField("territoryid", IntegerType(), False),
        StructField("territorydescription", StringType(), True),
        StructField("regionid", IntegerType(), True)
    ]))
])
region_kafka_schema = StructType([
    StructField("schema", StructType([])),
    StructField("payload", StructType([
        StructField("regionid", IntegerType(), False),
        StructField("regiondescription", StringType(), True)
    ]))
])

# 5. Parsing JSON
territory_parsed_df = territory_df \
    .filter(col('value').contains('"territoryid"')) \
    .select(from_json(col('value'), territory_kafka_schema).alias('json')) \
    .select("json.payload.*")

region_parsed_df = region_df \
    .filter(col('value').contains('"regionid"')) \
    .select(from_json(col('value'), region_kafka_schema).alias('json')) \
    .select("json.payload.*")

# 6. Daftarkan temporary view
territory_parsed_df.createOrReplaceTempView("territory")
region_parsed_df.createOrReplaceTempView("region")

# 7. Menulis query SQL untuk join
etl_query_df = spark.sql("""
    SELECT t.territoryid, t.territorydescription, t.regionid, r.regiondescription
    FROM territory t
    INNER JOIN region r
    ON t.regionid = r.regionid
""")

# 8. Fungsi untuk menulis ke PostgreSQL
def write_to_postgres(batch_df, batch_id):
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/postgres") \
        .option("dbtable", "postgres_table") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

# 9. Output ke PostgreSQL dengan foreachBatch
query = etl_query_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .option("checkpointLocation", "./data/checkpoint/region_territory") \
    .start()

query.awaitTermination()
