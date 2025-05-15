import os 

def write_data(df, ROOT_DIR, sub_directory, table_name):
    path = os.path.join(ROOT_DIR, "data", sub_directory, table_name)
    print(f"Writing {table_name.upper()} to {path}")
    df.write.format("delta").mode("overwrite").save(path)
    df.show(truncate=False)


def read_data(spark, ROOT_DIR, sub_directory, table_name):
    path = os.path.join(ROOT_DIR, "data", sub_directory, table_name)
    print(f"Loading {table_name.upper()} from {path}")
    return spark.read.format("delta").load(path)

