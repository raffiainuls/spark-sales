## Spark Sales  Data Pipeline 

#### Overview 
![flowchart_spark-sales](https://github.com/user-attachments/assets/6376beab-f636-4a7e-af14-0159aa8980ef)

This project is a Streaming and batching data pipeline project. This project using kafka for real-time data streaming and apache spark for ETL Streaming and batching for execute query table, and this project also use deltalake for storange data, and postgres for database.

#### Features 
- Producer Kafka: Python script read data table from csv and produce into kafka topics
- Stream Data Generation: Python script that streaming or produce data into kafka
- Streaming Pyspark Job: Python job Spark for execute Streaming job 
- Batching Pyspark Job: Python job Spark for execute query table schedulle
- DeltaLake Storange: Storange for saving data from Job Spark
- Python Function For Sink Postgres: Function python for sink data to Postgres
- Dagster Job Schedulling: Job Dagster for schedulling Pyspark Job


### Technologies Used 
- Python
- Apache Kafka
- Apache Spark
- DeltaLake
- Dagster
- Postgres


### Project Structure
<pre>  spark-sales/
   |-- .dagster/                      
       |-- .nux/                    
       |-- .telemetry/                    
       |-- history/               
       |-- logs/
       |-- schedules/
       |-- storage/
       |-- dagster.yaml
   |-- config/                   
       |-- config.yaml  
   |-- dagster_code/                   
       |-- jobs.py  
       |-- repository.py  
       |-- schedules.py  
   |-- data/                   
       |-- analytics/
       |-- checkpoints/
       |-- table/
       |-- warehouse/
   |-- database/                   # directory data for this project 
       |-- df_branch.csv                    
       |-- df_cust.csv                    
       |-- df_employee.csv              
       |-- df_order_status.csv
       |-- df_payment_status.csv                    
       |-- df_payment_method.csv                    
       |-- df_product.csv              
       |-- df_promotions.csv
       |-- df_sales.csv                    
       |-- df_schedule.csv              
       |-- df_shipping_status.csv
       |-- list_file.txt         # this file is useful for producer.py can know which csv file is used for 
   |-- etl/      
       |-- branch_daily_finance_performance/
            |-- branch_daily_finance_performance.py
            |-- sql.py
       |-- branch_finance_performance/
            |-- branch_finance_performance.py
            |-- sql.py
       |-- branch_monthly_finance_performance/
            |-- branch_monthly_finance_performance.py
            |-- sql.py
       |-- branch_performance/
            |-- branch_performance.py
            |-- sql.py
       |-- branch_weeakly_finance_performance/
            |-- branch_weeakly_finance_performance.py
            |-- sql.py
       |-- customers_retention/
            |-- customers_retention.py
            |-- sql.py
       |-- daily_finance_performance/
            |-- daily_finance_performance.py
            |-- sql.py
       |-- fact_employee/
            |-- fact_employee.py
            |-- sql.py
       |-- fact_sales/
            |-- fact_sales.py
            |-- sql.py
       |-- finance_performance/
            |-- finance_performance.py
            |-- sql.py
       |-- monthly_branch_performance/
            |-- monthly_branch_performance.py
            |-- sql.py
       |-- monthly_finance_performance/
            |-- monthly_finance_performance.py
            |-- sql.py
       |-- product_performance/
            |-- product_performance.py
            |-- sql.py
       |-- sum_transactions/
            |-- sum_transactions.py
            |-- sql.py
       |-- tbl_branch/
            |-- parser.py
            |-- shema.py
       |-- tbl_customers/
            |-- parser.py
            |-- shema.py
       |-- tbl_employee/
            |-- parser.py
            |-- shema.py
       |-- tbl_product/
            |-- parser.py
            |-- shema.py
       |-- tbl_promotions/
            |-- parser.py
            |-- shema.py
       |-- tbl_sales/
            |-- parser.py
            |-- shema.py
       |-- weeakly_finance_performance/
            |-- weeakly_finance_performance.py
            |-- sql.py
   |-- helper/     
            |-- write_read_delta.py
   |-- jars/     
            |-- delta-sharing-spark_2.12-3.3.1.jar
   |-- kafka/     
            |-- kafka_stream.py
   |-- postgres_writer/     
            |-- pg_writer.py
            |-- postgres_writer.py
   |-- spark/     
            |-- spark_session_batch.py
            |-- spark_session.py
   |-- .env  
   |-- ppyproject.toml
   |-- workspace.yaml/  
   |-- spark/  
   |-- last_id_backup              # backup list_id file
   |-- last_id.txt                 # this file save last_id that most_recent create in stream.py 
   |-- stream.py                   # file python that create data streaming and send into kafka 

  
