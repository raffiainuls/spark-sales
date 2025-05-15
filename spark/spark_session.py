from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
# this code used to create spark session 
# def get_spark_session():
#     spark = SparkSession.builder \
#         .appName("KafkaSparkStreamingETL") \
#         .master("local[2]") \
#         .config("spark.jars.packages", 
#                 "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,"  # Kafka connector
#                 "org.postgresql:postgresql:42.5.4") \
#         .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
#         .getOrCreate()

#     # Nonaktifkan native Hadoop library (opsional tergantung kebutuhan sistem)
#     # spark.sparkContext._jsc.hadoopConfiguration().set("hadoop.native.lib", "false")

#     return spark


def get_spark_session():
    # Membuat SparkSession dengan konfigurasi yang benar
    builder = SparkSession.builder \
        .appName("KafkaSparkStreamingETL") \
        .master("local[2]") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,"  # Kafka connector
                "org.postgresql:postgresql:42.5.4,"  # PostgreSQL JDBC driver
                "io.delta:delta-spark_2.12:3.1.0") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    # Membuat SparkSession
    spark = builder.getOrCreate()

    return spark
