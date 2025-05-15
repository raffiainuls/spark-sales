from spark.spark_session_batch import get_spark_session
from etl.monthly_branch_performance.sql import process_monthly_branch_performance
from postgres_writer.pg_writer import pg_writer
from postgres_writer.postgres_writer import PostgresWriter
import os 
from helper.write_read_delta import write_data, read_data

# 1. Spark Session untuk Batching
spark = get_spark_session()
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..",".."))

# 2. Baca data Delta Lake
fact_sales = read_data(spark, ROOT_DIR, "warehouse", "fact_sales_delta")
tbl_branch = read_data(spark, ROOT_DIR, "table", "tbl_branch")
# 3. Gunakan untuk analisis SQL
fact_sales.createOrReplaceTempView("fact_sales")
tbl_branch.createOrReplaceTempView("tbl_branch")

monthly_branch_performance = process_monthly_branch_performance(spark)

# 5. Tampilkan hasil analisis ke console
monthly_branch_performance.show(truncate=False)


# 6. (Opsional) Simpan hasil analisis batch ke Delta atau Parquet
# result.write.mode("overwrite").parquet("./data/analytics/summary_by_branch/")
# atau pakai Delta:
write_data(monthly_branch_performance,ROOT_DIR, "analytics", "monthly_branch_performance")
# 7. Sink Postgres
pg_writer = pg_writer()
pg_writer.write_to_postgres(monthly_branch_performance, table_name="monthly_branch_performance")


