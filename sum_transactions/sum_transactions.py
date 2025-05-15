from pyspark.sql import SparkSession 
from pyspark.sql.functions import from_json,to_json, col,struct, to_date, to_timestamp
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, BooleanType
import psycopg2

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

# 2. read Stream Kafka 


result_df = spark.sql("""
select * from tbl_sales
""")
# 8. Output ke console
query = result_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()

