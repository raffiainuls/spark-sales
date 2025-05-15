import psycopg2
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType


class PostgresWriter:
    def __init__(self, host, port, database, user, password):
        self.pg_host = host
        self.pg_port = port
        self.pg_database = database
        self.pg_user = user
        self.pg_password = password
        self.pg_url = f"jdbc:postgresql://{host}:{port}/{database}"
        self.pg_properties = {
            "user": user,
            "password": password,
            "driver": "org.postgresql.Driver"
        }

    def spark_schema_to_postgres(self, schema: StructType) -> str:
        type_mapping = {
            "StringType": "VARCHAR",
            "IntegerType": "INTEGER",
            "LongType": "BIGINT",
            "DateType": "DATE",
            "DoubleType": "DOUBLE PRECISION",
            "TimestampType": "TIMESTAMP",
            "BooleanType": "BOOLEAN",
            "FloatType": "REAL"
        }
        columns = []
        for field in schema.fields:
            spark_type = type(field.dataType).__name__
            pg_type = type_mapping.get(spark_type, "VARCHAR")
            columns.append(f"{field.name} {pg_type}")
        return ", ".join(columns)

    def create_table_if_not_exists(self, table_name: str, schema: StructType):
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {self.spark_schema_to_postgres(schema)}
        );
        """
        try:
            conn = psycopg2.connect(
                host=self.pg_host,
                port=self.pg_port,
                database=self.pg_database,
                user=self.pg_user,
                password=self.pg_password
            )
            cursor = conn.cursor()
            cursor.execute(create_table_sql)
            conn.commit()
            cursor.close()
            conn.close()
            print(f"Tabel '{table_name}' berhasil dicek/dibuat di PostgreSQL.")
        except Exception as e:
            print("Gagal membuat tabel:", e)
            raise

    def write_to_postgres(self, df: DataFrame, table_name: str, mode: str = "overwrite"):
        self.create_table_if_not_exists(table_name, df.schema)
        df.write \
            .mode(mode) \
            .jdbc(self.pg_url, table_name, properties=self.pg_properties)
