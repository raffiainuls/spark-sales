from spark.spark_session import get_spark_session
from etl.tbl_sales.parser import parse_tbl_sales
from etl.tbl_employee.parser import parse_tbl_employee
from etl.tbl_product.parser import parse_tbl_product
from etl.tbl_promotions.parser import parse_tbl_promotions
from etl.tbl_branch.parser import parse_tbl_branch
from kafka.kafka_stream import read_kafka_topics
from etl.fact_sales.sql import process_fact_sales
from etl.fact_employee.sql import process_fact_employee
from etl.sum_transactions.sql import process_sum_transactions
from etl.product_performance.sql import process_product_performance
from etl.branch_performance.sql import process_branch_performance
from etl.finance_performance.sql import process_finance_performance
from etl.branch_finance_performance.sql import process_branch_finance_performance
import threading 

# inisiasi spark session 
spark = get_spark_session()
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



# parse df
tbl_sales_df = parse_tbl_sales(tbl_sales_df)
tbl_employee_df = parse_tbl_employee(tbl_employee_df)
tbl_product_df = parse_tbl_product(tbl_product_df)
tbl_promotions_df = parse_tbl_promotions(tbl_promotions_df)
tbl_branch_df = parse_tbl_branch(tbl_branch_df)


tbl_sales_df.createOrReplaceTempView("tbl_sales")
tbl_employee_df.createOrReplaceTempView("tbl_employee")
tbl_product_df.createOrReplaceTempView("tbl_product")
tbl_promotions_df.createOrReplaceTempView("tbl_promotions")
tbl_branch_df.createOrReplaceTempView("tbl_branch")

###################### DEBUG TABLE ##########################

# query = tbl_sales_df.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .option("truncate", "false") \
#     .start()

print("TBL_PRODUCT")
tbl_product_df.write \
    .format("console") \
    .option("truncate", "false") \
    .save()
print("TBL_PROMOTIONS")
tbl_promotions_df.write \
    .format("console") \
    .option("truncate", "false") \
    .save()
print("TBL_EMPLOYEE")
tbl_employee_df.write \
    .format("console") \
    .option("truncate", "false") \
    .save()
print("TBL_BRANCH")
tbl_branch_df.write \
    .format("console") \
    .option("truncate", "false") \
    .save()


################## PROCESS FACT_EMPLOYEE ##########################
fact_employee = process_fact_employee(spark)

fact_employee.createOrReplaceTempView("fact_employee")

tbl_employee_df.write \
    .format("console") \
    .option("truncate", "false") \
    .save()

################## PROCESS FACT_SALES ##########################


# query fact_sales
fact_sales = process_fact_sales(spark)
# create tempView fact_sales
fact_sales.createOrReplaceTempView("fact_sales")

query = fact_sales.writeStream \
    .format("parquet") \
    .option("path", "./data/warehouse/fact_sales/") \
    .option("checkpointLocation", "./data/checkpoints/fact_sales/") \
    .outputMode("append") \
    .start()


# query = fact_sales.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .option("truncate", "false") \
#     .start()

# query.awaitTermination()

################## PROCESS SUM_TRANSACTIONS ##########################
#query sum_transactions
sum_transactions = process_sum_transactions(spark)
# create tempView fact_sales
sum_transactions.createOrReplaceTempView("sum_transactions")

query = sum_transactions.writeStream \
    .format("parquet") \
    .option("path", "./data/warehouse/sum_transactions/") \
    .option("checkpointLocation", "./data/checkpoints/sum_transactions/") \
    .outputMode("append") \
    .start()

# # query = sum_transactions.writeStream \
# #     .outputMode("append") \
# #     .format("console") \
# #     .option("truncate", "false") \
# #     .start()

query.awaitTermination()

# # tbl_employee_df.awaitTermination()

# ################## PROCESS PRODUCT_PERFORMANCE ##########################
# product_performance = process_product_performance(spark)
# product_performance.createOrReplaceTempView("product_performance")

# # query = product_performance.writeStream \
# #     .outputMode("Update") \
# #     .format("console") \
# #     .option("truncate", "false") \
# #     .start()

# # query.awaitTermination()

# ################## PROCESS BRANCH_PERFORMANCE ##########################
# branch_performance = process_branch_performance(spark)
# branch_performance.createOrReplaceTempView("branch_performance")

# # query = branch_performance.writeStream \
# #     .outputMode("Update") \
# #     .format("console") \
# #     .option("truncate", "false") \
# #     .start()

# # query.awaitTermination()

# ################## PROCESS FINANCE_PERFORMANCE ##########################
# finance_performance = process_finance_performance(spark)
# finance_performance.createOrReplaceTempView("finance_performance")

# # query = finance_performance.writeStream \
# #     .outputMode("Update") \
# #     .format("console") \
# #     .option("truncate", "false") \
# #     .start()

# # query.awaitTermination()

# ################## PROCESS BRANCH_FINANCE_PERFORMANCE ##########################
# # branch_finance_performance = process_branch_finance_performance(spark)
# # branch_finance_performance.createOrReplaceTempView("branch_finance_performance")

# # query = branch_finance_performance.writeStream \
# #     .outputMode("append") \
# #     .format("console") \
# #     .option("truncate", "false") \
# #     .start()

# # query.awaitTermination()




