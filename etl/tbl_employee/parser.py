from pyspark.sql.functions import col, from_json
from .schema import tbl_employee_schema

def parse_tbl_employee(df):
    return df.selectExpr("CAST(value AS STRING)") \
             .select(from_json(col("value"), tbl_employee_schema).alias("json")) \
             .select("json.payload.*")
