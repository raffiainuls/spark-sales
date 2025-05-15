from pyspark.sql.functions import col, from_json
from .schema import tbl_customers_schema

def parse_tbl_customers(df):
    return df.selectExpr("CAST(value AS STRING)") \
             .select(from_json(col("value"), tbl_customers_schema).alias("json")) \
             .select("json.payload.*")
