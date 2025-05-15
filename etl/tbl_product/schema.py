from pyspark.sql.types import *

tbl_product_schema = StructType([
    StructField("schema", StructType([])),
    StructField("payload", StructType([
        StructField("id", IntegerType(), True),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("sub_category", StringType(), True),
        StructField("price", LongType(), True),
        StructField("profit", IntegerType(), True),
        StructField("stock", IntegerType(), True),
        StructField("created_time", TimestampType(), True),
        StructField("modified_time", TimestampType(), True)
]))])
