from spark.spark_session_batch import get_spark_session
from etl.product_performance.sql import process_product_performance
from postgres_writer.pg_writer import pg_writer
from postgres_writer.postgres_writer import PostgresWriter
import os 
from helper.write_read_delta import write_data, read_data

# 1. Spark Session untuk Batching
spark = get_spark_session()
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..",".."))

# 2. Baca data Delta Lake
fact_sales = read_data(spark, ROOT_DIR, "warehouse", "fact_sales_delta")
tbl_product = read_data(spark, ROOT_DIR, "table", "tbl_product")
# 3. Gunakan untuk analisis SQL
fact_sales.createOrReplaceTempView("fact_sales")
tbl_product.createOrReplaceTempView("tbl_product")

product_performance = process_product_performance(spark)
product_performance.show(truncate=False)

# 6. (Opsional) Simpan hasil analisis batch ke Delta atau Parquet
# atau pakai Delta:
write_data(product_performance,ROOT_DIR, "analytics", "product_performance")
# 7. Sink Postgres
pg_writer = pg_writer()
pg_writer.write_to_postgres(product_performance, table_name="product_performance")


