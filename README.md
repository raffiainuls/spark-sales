## Spark Sales  Data Pipeline 

#### Overview 
![flowchart_fix](https://github.com/user-attachments/assets/c3746ce5-6f82-4be4-854a-70a28c4baf1c)


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
   |-- .dagster/                      # Directory Configuration dagster
       |-- .nux/                    
       |-- .telemetry/                    
       |-- history/               
       |-- logs/
       |-- schedules/
       |-- storage/
       |-- dagster.yaml
   |-- config/                      # Directory config.yaml for configuration stream.py (streaming code)
       |-- config.yaml  
   |-- dagster_code/                # Directory  Dagster
       |-- jobs.py                  # configurasi list job that run in dagster
       |-- repository.py            # file repository Dagster
       |-- schedules.py             # Configurasi schedulling dagster
   |-- data/                        # directory for storange deltalake
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
       |-- producer.py           # file python for send data in file csv to kafka 
       |-- list_file.txt         # this file is useful for producer.py can know which csv file is used for 
   |-- etl/                      # directory etl or spark job 
       |-- branch_daily_finance_performance/
            |-- branch_daily_finance_performance.py            # Main python file for etl branch_daily_finance_performance
            |-- sql.py                                         # python file contains sql for branch_daily_finance_performance
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
            |-- parser.py               # function parser for casting data from kafka
            |-- shema.py                # schema definition for tbl_branch from kafka
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
            |-- write_read_delta.py                     # function for write and read data from deltalake
   |-- jars/     
            |-- delta-sharing-spark_2.12-3.3.1.jar      # jar deltalake for spark 
   |-- kafka/     
            |-- kafka_stream.py                         # function spark for consume data from kafka in streaming and batching
   |-- postgres_writer/                                 # directory function for sink to postgres
            |-- pg_writer.py
            |-- postgres_writer.py
   |-- spark/                                           # directory for function spark session in streaming in batching 
            |-- spark_session_batch.py
            |-- spark_session.py
   |-- .env  
   |-- ppyproject.toml
   |-- workspace.yaml/  
   |-- spark/  
   |-- last_id_backup              # backup list_id file
   |-- last_id.txt                 # this file save last_id that most_recent create in stream.py 
   |-- stream.py                   # file python that create data streaming and send into kafka 
 </pre>


### Project Workflow 
1. Producer Kafka
   - python script (/spark-sales/database/producer.py) will read list file csv in list_file.txt that containing database tables that saves into csv, and then from the list file csv python script will access csv file and then produce data in file csv into kafka. table that send to kafka such as : ```tbl_order_status, tbl_payment_method, tb_payment_status, tbl_shipping_status, tbl_employee, tbl_promotions, tbl_sales, tbl_product, tbl_schedulle_emp, tbl_customers, and tbl_branch``` and in this project all table above save into delta lake in directory ```/spark-sales/data/table/``` and this data delta lake used to load data on several spark jobs
2. Stream Data Generation
   - This python script if it run will generate data and produce data into kafka topics table tbl_sales, this python script also have metrics thatis adjusted to the table in the csv file that was previously sent to kafka, so the generated data contains several calculations for several fields so that the data produced is not too random.
3. Streaming Pyspark Job
   - in directory ```/spark-sales/etl/``` it is directory for all spark job and in this project there is job for streaming, in this project there is table fact_sales and this table will be streaming, this job will load data table in delta lake ```/spark-sales/data/table/``` and execute query for fact_sales and then save data fact_sales into delta lake ```/spark-sales/data/werehouse/fact_sales_delta/``` and the job for this streaming is always running.
4. Batching Pyspark Job
   - In this project besides streaming job there are also severals job for batching job, this job will be load data from delta lake in ```/spark-sales/data/werehouse/fact_sales_delta/``` and also data table in ```/spark-sales/data/table/``` after load data table from delta lake the jobs will be execute query for any tables and save it data table into delta lake ```/spark-sales/data/analytics/```.
5. DeltaLake Storange
   - In this project delta lake save in local directory in ```/spark-sales/data/``` in this directory  store data from source tables, streaming job result tables and also batching job result tables.
6. Dagster Job Schedulling
   - In this project use dagster for schedulling batching Pyspark Job
7. Python Function For Sink Postgres
   - In batching jobs, besides saving data into Delta Lake, there is also a function to sink data into Postgres.
  
### Instalation & Setup 
#### Prerequisites 
- Apache Spark
- Zookeeper & Kafka (in this project i use kafka and zookeeper in local windows)
- Python
- Delta Lake
- Postgres
- Dagster

### Steps 
1. clone this repository
   ```bash
   https://github.com/raffiainuls/spark-sales
2. prepare zookeeper & kafka first, and make sure your zookeeper and kafka allredy running correctly
3. wait until zookeeper and kafka already running correctly, you can running ```/spark-sales/database/producer.py``` this file will send data in csv file that lists in list_file.txt into kafka
4. if all topics for each table already available in kafka you can run ```/flink-sales/stream.py``` this file will generate data streaming into topic tbl_sales in kafka. in this python script there is some calculations metrics for generate value in some field, so the value that produce not too random.

 

  
