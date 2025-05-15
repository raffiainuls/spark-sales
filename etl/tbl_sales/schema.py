from pyspark.sql.types import *

tbl_sales_schema = StructType([StructField("payload", StructType([
    StructField("id", IntegerType(), False),
    StructField("product_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("branch_id", IntegerType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("payment_method", IntegerType(), False),
    StructField("order_date", TimestampType(), False),
    StructField("order_status", FloatType(), True),
    StructField("payment_status", FloatType(), True),
    StructField("shipping_status", FloatType(), True),
    StructField("is_online_transaction", BooleanType(), True),
    StructField("delivery_fee", IntegerType(), True),
    StructField("is_free_delivery_fee", StringType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("modified_at", TimestampType(), True),
]))])
