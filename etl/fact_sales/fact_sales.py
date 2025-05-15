from spark.spark_session import get_spark_session
from etl.tbl_sales.parser import parse_tbl_sales
from etl.tbl_employee.parser import parse_tbl_employee
from etl.tbl_product.parser import parse_tbl_product
from etl.tbl_promotions.parser import parse_tbl_promotions
from etl.tbl_branch.parser import parse_tbl_branch
from etl.tbl_customers.parser import parse_tbl_customers
from kafka.kafka_stream import read_kafka_topics
from etl.fact_sales.sql import process_fact_sales
from delta.tables import DeltaTable
import os
from helper.write_read_delta import write_data
from postgres_writer.pg_writer import pg_writer
from postgres_writer.postgres_writer import PostgresWriter

# inisiasi spark session 
spark = get_spark_session()
#inisiasi kafka
kafka_streams, kafka_statics = read_kafka_topics(spark)

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..",".."))


#consume topic and create dataframe
tbl_sales_df = kafka_streams.get("table.public.tbl_sales")
tbl_promotions_df = kafka_statics.get("table.public.tbl_promotions")
tbl_product_df = kafka_statics.get("table.public.tbl_product")
tbl_customers_df = kafka_statics.get("table.public.tbl_customers") 
tbl_employee_df = kafka_statics.get("table.public.tbl_employee")
tbl_branch_df = kafka_statics.get("table.public.tbl_branch")


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

write_data(tbl_product_df, ROOT_DIR, "table", "tbl_product")
write_data(tbl_promotions_df,ROOT_DIR, "table", "tbl_promotions")
write_data(tbl_employee_df,ROOT_DIR, "table", "tbl_employee")
write_data(tbl_branch_df,ROOT_DIR, "table", "tbl_branch")
write_data(tbl_customers_df,ROOT_DIR, "table", "tbl_customers")


################## PROCESS FACT_SALES ##########################


fact_sales = process_fact_sales(spark)
# create temp view for fact_sales
fact_sales.createOrReplaceTempView("fact_sales")

# define path delta 
DATA_DIR = os.path.join(ROOT_DIR, "data")
delta_path_fact_sales = os.path.join(DATA_DIR, "warehouse", "fact_sales_delta")
CHECKPOINT_DIR = os.path.join(DATA_DIR, "checkpoints", "fact_sales")

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
        .option("checkpointLocation", CHECKPOINT_DIR) \
        .option("path", delta_path_fact_sales) \
        .foreachBatch(upsert_to_delta) \
        .start()
else:
    # if there is no, save for the first time 
    print("FACT_SALES")
    query = fact_sales.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", CHECKPOINT_DIR) \
        .option("path", delta_path_fact_sales) \
        .start()
    
query.awaitTermination()

