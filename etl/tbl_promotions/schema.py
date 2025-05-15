from pyspark.sql.types import *

tbl_promotions_schema = StructType([
    StructField("schema", StructType([])),
    StructField("payload", StructType([
        StructField("id", IntegerType(), True),
        StructField("event_name", StringType(), True),
        StructField("disc", IntegerType(), True),
        StructField("time", DateType(), True),  
        StructField("created_at", TimestampType(), True)
]))])
