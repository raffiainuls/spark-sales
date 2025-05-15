from spark.spark_session_batch import get_spark_session
from etl.weekly_finance_performance.sql import process_weekly_finance_performance
from postgres_writer.pg_writer import pg_writer
from postgres_writer.postgres_writer import PostgresWriter

# 1. Spark Session untuk Batching
spark = get_spark_session()

# 2. Baca data Delta Lake
fact_sales = spark.read.format("delta").load("./data/warehouse/fact_sales_delta/")
sum_transactions = spark.read.format("delta").load("./data/analytics/sum_transactions/")
tbl_product = spark.read.format("delta").load("./data/table/tbl_product/")

# 3. Gunakan untuk analisis SQL
fact_sales.createOrReplaceTempView("fact_sales")
sum_transactions.createOrReplaceTempView("sum_transactions")
tbl_product.createOrReplaceTempView("tbl_product")

weekly_finance_performance = process_weekly_finance_performance(spark)
weekly_finance_performance.show(truncate=False)

# 6. (Opsional) Simpan hasil analisis batch ke Delta atau Parquet
weekly_finance_performance.write.format("delta").mode("overwrite").save("./data/analytics/weekly_finance_performance/")
# 7. Sink Postgres
pg_writer = pg_writer()
pg_writer.write_to_postgres(weekly_finance_performance, table_name="weekly_finance_performance")


