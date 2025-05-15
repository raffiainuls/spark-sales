from pyspark.sql.functions import col, from_json
from .schema import tbl_product_schema

def parse_tbl_product(df):
    return df.selectExpr("CAST(value AS STRING)") \
             .select(from_json(col("value"), tbl_product_schema).alias("json")) \
             .select("json.payload.*")
