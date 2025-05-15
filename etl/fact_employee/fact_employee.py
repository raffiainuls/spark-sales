from spark.spark_session_batch import get_spark_session
from etl.fact_employee.sql import process_fact_employee
from postgres_writer.pg_writer import pg_writer
from postgres_writer.postgres_writer import PostgresWriter
import os 
from helper.write_read_delta import write_data, read_data

# 1. Spark Session untuk Batching
spark = get_spark_session()
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..",".."))

# 2. Baca data Delta Lake
tbl_employee = read_data(spark, ROOT_DIR, "table", "tbl_employee")
# 3. Gunakan untuk analisis SQL
tbl_employee.createOrReplaceTempView("tbl_employee")

fact_employee = process_fact_employee(spark)
fact_employee.show(truncate=False)

# 6. (Opsional) Simpan hasil analisis batch ke Delta atau Parquet
write_data(fact_employee,ROOT_DIR, "analytics", "fact_employee")
# 7. Sink Postgres
pg_writer = pg_writer()
pg_writer.write_to_postgres(fact_employee, table_name="fact_employee")


