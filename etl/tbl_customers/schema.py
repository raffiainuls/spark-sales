from pyspark.sql.types import *

tbl_customers_schema = StructType([StructField("payload", StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), False),
    StructField("address", StringType(), False),
    StructField("phone", StringType(), False),
    StructField("email", StringType(), False),
    StructField("created_at", TimestampType(), True),
    StructField("modified_at", TimestampType(), True),
]))])
