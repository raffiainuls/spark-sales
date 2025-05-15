from pyspark.sql.functions import col, from_json
from .schema import tbl_promotions_schema

def parse_tbl_promotions(df):
    return df.selectExpr("CAST(value AS STRING)") \
             .select(from_json(col("value"), tbl_promotions_schema).alias("json")) \
             .select("json.payload.*")
