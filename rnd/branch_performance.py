from spark.spark_session_batch import get_spark_session
from etl.branch_performance.sql import process_branch_performance
from postgres_writer.pg_writer import pg_writer
from postgres_writer.postgres_writer import PostgresWriter

# 1. Spark Session untuk Batching
spark = get_spark_session()

# 2. Baca data Delta Lake
fact_sales = spark.read.format("delta").load("./data/warehouse/fact_sales_delta/")
tbl_branch = spark.read.format("delta").load("./data/table/tbl_branch/")

# 3. Gunakan untuk analisis SQL
fact_sales.createOrReplaceTempView("fact_sales")
tbl_branch.createOrReplaceTempView("tbl_branch")

branch_performance = process_branch_performance(spark)
branch_performance.show(truncate=False)

# 6. (Opsional) Simpan hasil analisis batch ke Delta atau Parquet
# atau pakai Delta:
branch_performance.write.format("delta").mode("overwrite").save("./data/analytics/branch_performance/")
# 7. Sink Postgres
pg_writer = pg_writer()
pg_writer.write_to_postgres(branch_performance, table_name="branch_performance")


