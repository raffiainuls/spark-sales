from spark.spark_session import get_spark_session
from etl.tbl_sales.parser import parse_tbl_sales
from etl.tbl_employee.parser import parse_tbl_employee
from etl.tbl_product.parser import parse_tbl_product
from etl.tbl_promotions.parser import parse_tbl_promotions
from etl.tbl_branch.parser import parse_tbl_branch
from etl.tbl_customers.parser import parse_tbl_customers
from kafka.kafka_stream import read_kafka_topics
from etl.fact_sales.sql import process_fact_sales
from etl.fact_employee.sql import process_fact_employee
from etl.sum_transactions.sql import process_sum_transactions
from etl.product_performance.sql import process_product_performance
from etl.branch_performance.sql import process_branch_performance
from etl.finance_performance.sql import process_finance_performance
from etl.branch_finance_performance.sql import process_branch_finance_performance
import threading 
from delta.tables import DeltaTable

# inisiasi spark session 
spark = get_spark_session()
print(f"ini yaaaaa {spark.version}")
#inisiasi kafka
kafka_streams, kafka_statics = read_kafka_topics(spark)

#consume topic and create dataframe
tbl_sales_df = kafka_streams.get("table.public.tbl_sales")
tbl_promotions_df = kafka_statics.get("table.public.tbl_promotions")
tbl_product_df = kafka_statics.get("table.public.tbl_product")
tbl_customers_df = kafka_statics.get("table.public.tbl_customers") 
tbl_employee_df = kafka_statics.get("table.public.tbl_employee")
tbl_branch_df = kafka_statics.get("table.public.tbl_branch")

print("Kafka Static Keys:", kafka_statics.keys())
print("############## DEBUG ##################")
print(tbl_employee_df)

# parse df
tbl_sales_df = parse_tbl_sales(tbl_sales_df)
tbl_employee_df = parse_tbl_employee(tbl_employee_df)
tbl_product_df = parse_tbl_product(tbl_product_df)
tbl_promotions_df = parse_tbl_promotions(tbl_promotions_df)
tbl_branch_df = parse_tbl_branch(tbl_branch_df)
tbl_customers_df = parse_tbl_customers(tbl_customers_df)


tbl_sales_df.createOrReplaceTempView("tbl_sales")
tbl_employee_df.createOrReplaceTempView("tbl_employee")
tbl_product_df.createOrReplaceTempView("tbl_product")
tbl_promotions_df.createOrReplaceTempView("tbl_promotions")
tbl_branch_df.createOrReplaceTempView("tbl_branch")
tbl_customers_df.createOrReplaceTempView("tbl_customers")

###################### DEBUG TABLE ##########################

query = tbl_sales_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()
print("TBL_PRODUCT")
tbl_product_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save("./data/table/tbl_product/")
tbl_product_df.show(truncate=False)

print("TBL_PROMOTIONS")
tbl_promotions_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save("./data/table/tbl_promotions/")
tbl_promotions_df.show(truncate=False)

print("TBL_EMPLOYEE")
tbl_employee_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save("./data/table/tbl_employee/")
tbl_employee_df.show(truncate=False)

print("TBL_BRANCH")
tbl_branch_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save("./data/table/tbl_branch/")
tbl_branch_df.show(truncate=False)

print("TBL_CUSTOMERS")
tbl_customers_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save("./data/table/tbl_customers/")
tbl_customers_df.show(truncate=False)


################## PROCESS FACT_SALES ##########################


fact_sales = process_fact_sales(spark)
# create temp view for fact_sales
fact_sales.createOrReplaceTempView("fact_sales")

# define path delta 
delta_path_fact_sales = "./data/warehouse/fact_sales_delta/"

if DeltaTable.isDeltaTable(spark, delta_path_fact_sales):
    delta_table = DeltaTable.forPath(spark, delta_path_fact_sales)

    def upsert_to_delta(microBatchOutputDF, batchId):
        delta_table.alias("target").merge(
            microBatchOutputDF.alias("source"),
            "target.id = source.id" 
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()
    print("FACT_SALES")
    query = fact_sales.writeStream \
        .format("delta") \
        .outputMode("update") \
        .option("checkpointLocation", "./data/checkpoints/fact_sales/") \
        .option("path", delta_path_fact_sales) \
        .foreachBatch(upsert_to_delta) \
        .start()
else:
    # if there is no, save for the first time 
    print("FACT_SALES")
    query = fact_sales.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", "./data/checkpoints/fact_sales/") \
        .option("path", delta_path_fact_sales) \
        .start()
    
query.awaitTermination()

