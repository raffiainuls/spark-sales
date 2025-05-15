from pyspark.sql import SparkSession
def get_spark_session():
    # Membuat SparkSession dengan konfigurasi yang benar
    builder = SparkSession.builder \
        .appName("BatchAnalysist") \
        .config("spark.jars.packages",
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,"  # Kafka connector
                    "org.postgresql:postgresql:42.5.4,"  # PostgreSQL JDBC driver
                    "io.delta:delta-spark_2.12:3.1.0") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \

    # Membuat SparkSession
    spark = builder.getOrCreate()

    return spark
