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

    def create_table_if_not_exists(self, table_name: str, schema: StructType, primary_key: str):
        import psycopg2

        column_definitions = self.spark_schema_to_postgres(schema)
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {column_definitions}
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

            # Step 1: Create table if not exists
            cursor.execute(create_table_sql)
            conn.commit()

            # Step 2: Check if primary key already exists
            cursor.execute(f"""
                SELECT constraint_name 
                FROM information_schema.table_constraints 
                WHERE table_name = %s 
                AND constraint_type = 'PRIMARY KEY';
            """, (table_name,))
            pk_exists = cursor.fetchone()

            # Step 3: If no PK, add it
            if not pk_exists:
                constraint_name = f"{table_name}_{primary_key}_pk"
                alter_sql = f"""
                    ALTER TABLE {table_name}
                    ADD CONSTRAINT {constraint_name} PRIMARY KEY ({primary_key});
                """
                cursor.execute(alter_sql)
                conn.commit()
                print(f"Primary key ditambahkan ke tabel '{table_name}' pada kolom '{primary_key}'.")
            else:
                print(f"Primary key sudah ada untuk tabel '{table_name}'.")

            cursor.close()
            conn.close()
            print(f"Tabel '{table_name}' berhasil dicek/dibuat di PostgreSQL.")

        except Exception as e:
            print("Gagal membuat/alter tabel:", e)
            raise


    def write_to_postgres(self, df: DataFrame, table_name: str, primary_key: str):
        import pandas as pd
        from sqlalchemy import create_engine, text

        # Buat tabel jika belum ada, dengan primary key
        self.create_table_if_not_exists(table_name, df.schema, primary_key)

        if df.count() == 0:
            print("DataFrame kosong, tidak ada data yang ditulis.")
            return

        pdf: pd.DataFrame = df.toPandas()

        engine = create_engine(
            f'postgresql://{self.pg_user}:{self.pg_password}@{self.pg_host}:{self.pg_port}/{self.pg_database}'
        )

        with engine.begin() as conn:
            for _, row in pdf.iterrows():
                data_dict = row.to_dict()
                columns = ', '.join(data_dict.keys())
                values = ', '.join([repr(value) for value in data_dict.values()])
                updates = ', '.join([f"{col}=EXCLUDED.{col}" for col in data_dict.keys() if col != primary_key])

                query = f"""
                    INSERT INTO {table_name} ({columns})
                    VALUES ({values})
                    ON CONFLICT ({primary_key}) 
                    DO UPDATE SET {updates};
                """

                try:
                    conn.execute(text(query))
                except Exception as e:
                    print(f"Error executing query for row {data_dict}: {e}")
                    continue

        print(f"{len(pdf)} baris berhasil di-upsert ke tabel '{table_name}'.")
