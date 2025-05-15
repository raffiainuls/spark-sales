from spark.spark_session_batch import get_spark_session
from etl.sum_transactions.sql import process_sum_transactions
from postgres_writer.pg_writer import pg_writer
from postgres_writer.postgres_writer import PostgresWriter

# 1. Spark Session untuk Batching
spark = get_spark_session()

# 2. Baca data Delta Lake
fact_sales = spark.read.format("delta").load("./data/warehouse/fact_sales_delta/")

# 3. Gunakan untuk analisis SQL
fact_sales.createOrReplaceTempView("fact_sales")
fact_sales.show(truncate=False)
sum_transactions = process_sum_transactions(spark)


# 4. Tampilkan hasil analisis ke console
sum_transactions.show(truncate=False)


# 5.Simpan hasil analisis batch ke Delta atau Parquet
sum_transactions.write.format("delta").mode("overwrite").save("./data/analytics/sum_transactions/")
# 6. Sink Postgres
pg_writer = pg_writer()
pg_writer.write_to_postgres(sum_transactions, table_name="sum_transactions")


