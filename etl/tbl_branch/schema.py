from pyspark.sql.types import *

tbl_branch_schema = StructType([StructField("payload", StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("location", StringType(), True),
    StructField("address", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("created_time", TimestampType(), True),
    StructField("modified_time", TimestampType(), True),
]))])
