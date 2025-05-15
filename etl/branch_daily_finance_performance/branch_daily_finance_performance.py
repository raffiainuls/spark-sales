from spark.spark_session_batch import get_spark_session
from etl.branch_daily_finance_performance.sql import process_branch_daily_finance_performance
from postgres_writer.pg_writer import pg_writer
from postgres_writer.postgres_writer import PostgresWriter
import os 
from helper.write_read_delta import write_data, read_data

# 1. Spark Session for Batching
spark = get_spark_session()
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..",".."))

# 2. Baca data Delta Lake
fact_sales = read_data(spark, ROOT_DIR, "warehouse", "fact_sales_delta")
sum_transactions = read_data(spark, ROOT_DIR, "analytics", "sum_transactions")
tbl_branch = read_data(spark, ROOT_DIR, "table", "tbl_branch")
tbl_product = read_data(spark, ROOT_DIR, "table", "tbl_product")

# 3. Gunakan untuk analisis SQL
fact_sales.createOrReplaceTempView("fact_sales")
sum_transactions.createOrReplaceTempView("sum_transactions")
tbl_branch.createOrReplaceTempView("tbl_branch")
tbl_product.createOrReplaceTempView("tbl_product")

branch_daily_finance_performance = process_branch_daily_finance_performance(spark)

# 5. Tampilkan hasil analisis ke console
branch_daily_finance_performance.show(truncate=False)


# 6. (Opsional) Simpan hasil analisis batch ke Delta 
write_data(branch_daily_finance_performance,ROOT_DIR, "analytics", "branch_daily_finance_performance")
# 6. Sink Postgres
pg_writer = pg_writer()
pg_writer.write_to_postgres(branch_daily_finance_performance, table_name="branch_daily_finance_performance")
