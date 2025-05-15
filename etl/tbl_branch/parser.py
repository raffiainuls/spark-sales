from pyspark.sql.functions import col, from_json
from .schema import tbl_branch_schema

def parse_tbl_branch(df):
    return df.selectExpr("CAST(value AS STRING)") \
             .select(from_json(col("value"), tbl_branch_schema).alias("json")) \
             .select("json.payload.*")
