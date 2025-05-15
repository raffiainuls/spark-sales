from pyspark.sql import SparkSession
#from utils.config_loader import load_config
import yaml

def load_config(path="./config/config.yaml"):
    with open(path, 'r') as file:
        return yaml.safe_load(file)

def read_kafka_topics(spark: SparkSession, config_path: str = "./config/config.yaml"):
    config = load_config(config_path)
    bootstrap_servers = config["kafka"]["bootstrap_servers"]
    stream_topics = config["kafka"].get("stream_topics", [])
    static_topics = config["kafka"].get("static_topics", [])

    kafka_streams = {}
    kafka_statics = {}

    for topic in stream_topics:
        df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", bootstrap_servers) \
            .option("subscribe", f"{topic}") \
            .option("startingOffsets", "earliest") \
            .load() 
        kafka_streams[topic] = df

    for topic in static_topics:
        df = spark.read \
            .format("kafka") \
            .option("kafka.bootstrap.servers", bootstrap_servers) \
            .option("subscribe", f"{topic}") \
            .option("startingOffsets", "earliest") \
            .load() 
        kafka_statics[topic] = df

    return kafka_streams, kafka_statics
