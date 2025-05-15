from pyspark.sql.types import *

tbl_employee_schema = StructType([
    StructField("schema", StructType([])),  # bagian schema tetap dikosongkan
    StructField("payload", StructType([
        StructField("id", IntegerType(), True),
        StructField("branch_id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("salary", IntegerType, True),
        StructField("active", BooleanType(), True),
        StructField("address", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("email", StringType(), True),
        StructField("created_at", StringType(), True),
    ]))
])
