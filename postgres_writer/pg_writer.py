from .postgres_writer import PostgresWriter

def pg_writer():
    return PostgresWriter(
        host="localhost",
        port=5432,
        database="sales-project",
        user="postgres",
        password="postgres"
    )