from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common import WatermarkStrategy, Types
from sensor_row_assembler import SensorRowAssembler
from neighbor_feature_builder import NeighborFeatureBuilder

import json
import os
import urllib.parse


env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

connector = os.path.join(BASE_DIR, "flink_lib/flink-connector-kafka-3.3.0-1.20.jar")
client = os.path.join(BASE_DIR, "flink_lib/kafka-clients-3.7.0.jar")

connector = urllib.parse.quote(connector)
client = urllib.parse.quote(client)

env.add_jars(
    f"file://{connector}",
    f"file://{client}"
)

source = (
    KafkaSource.builder()
    .set_bootstrap_servers("localhost:9092")
    .set_topics(
        "pm10",
        "pm25",
        "temperature",
        "humidity",
        "pressure",
        "no2",
        "co",
        "o3",
        "wind_speed"
    )
    .set_group_id("flink-consumer")
    .set_starting_offsets(KafkaOffsetsInitializer.latest())
    .set_value_only_deserializer(SimpleStringSchema())
    .build()
)

stream = env.from_source(
    source,
    WatermarkStrategy.no_watermarks(),
    "Kafka Source"
)

def parse_event(event):
    return json.loads(event)

parsed_stream = stream.map(parse_event)

assembled_stream = (
    parsed_stream
    .key_by(lambda x: str(x["sensorId"]))
    .process(SensorRowAssembler())
)

enriched_stream = (
    assembled_stream
    .key_by(lambda x: x["city"])
    .process(NeighborFeatureBuilder())
)

enriched_stream.print()

print("Flink job is starting...")
print("Waiting for Kafka events...")

env.execute("Sensor Stream Test")