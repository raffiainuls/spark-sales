from pyspark.sql import SparkSession

# 1. Spark Session untuk Batching
spark = SparkSession.builder \
    .appName("BatchJobEmployeeAnalysis") \
    .getOrCreate()

# 2. Baca data hasil streaming
fact_sales = spark.read.parquet("./data/warehouse/fact_sales/")

# 3. Gunakan untuk analisis SQL
fact_sales.createOrReplaceTempView("fact_sales")

# 4. Contoh Query
# result = spark.sql("""
#     SELECT position, COUNT(*) AS total_karyawan
#     FROM fact_sales
# """)


fact_sales.write \
    .format("console") \
    .option("truncate", "false") \
    .save()

# 5. Simpan hasil analisis batch
# result.write.mode("overwrite").parquet("/data/analytics/employee_position_summary/")
