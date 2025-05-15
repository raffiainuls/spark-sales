from pyspark.sql import SparkSession

# 1. Spark Session untuk Batching
spark = SparkSession.builder \
    .appName("BatchJobEmployeeAnalysis") \
    .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,"  # Kafka connector
                "org.postgresql:postgresql:42.5.4,"  # PostgreSQL JDBC driver
                "io.delta:delta-spark_2.12:3.1.0") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# 2. Baca data Delta Lake
fact_sales = spark.read.format("delta").load("./data/warehouse/fact_sales_delta/")
tbl_product = spark.read.format("delta").load("./data/table/tbl_product/")
# tbl_sales = spark.read.format("delta").load("./data/table/tbl_sales/")

# 3. Gunakan untuk analisis SQL
fact_sales.createOrReplaceTempView("fact_sales")
tbl_product.createOrReplaceTempView("tbl_product")
# tbl_sales.createOrReplaceTempView("tbl_sales")


# # 4. Contoh Query
result = spark.sql("""
        select 
        fs2.product_id,
        tp.product_name,
        sum(quantity) as jumlah_terjual 
        from fact_sales fs2 
        left join tbl_product tp 
        on tp.id  = fs2.product_id 
        group by 1,2""")

# result = spark.sql("""
#         select * from fact_sales where id > 10620""")




# 5. Tampilkan hasil analisis ke console
result.show(truncate=False)

# 6. Convert Spark DataFrame ke Pandas DataFrame
# pandas_df = result.toPandas()


# sorted_pandas_df = pandas_df.sort_values(by="id")


# sorted_pandas_df.to_excel("./fact_sales_analysis_sorted.xlsx", index=False)

# print("Hasil telah disimpan ke Excel setelah sorting!")


# 6. (Opsional) Simpan hasil analisis batch ke Delta atau Parquet
# result.write.mode("overwrite").parquet("./data/analytics/summary_by_branch/")
# atau pakai Delta:
# result.write.format("delta").mode("overwrite").save("./data/analytics/summary_by_branch/")
