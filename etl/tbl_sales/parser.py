from pyspark.sql.functions import col, from_json
from .schema import tbl_sales_schema

def parse_tbl_sales(df):
    return df.selectExpr("CAST(value AS STRING)") \
             .select(from_json(col("value"), tbl_sales_schema).alias("json")) \
             .select("json.payload.*")
