from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

import config

_sr_client = SchemaRegistryClient({
    "url": config.SCHEMA_REGISTRY_URL,
    "basic.auth.user.info": (
        f"{config.SCHEMA_REGISTRY_API_KEY}:{config.SCHEMA_REGISTRY_API_SECRET}"
    ),
})

_avro_deserializer = AvroDeserializer(_sr_client)

_consumer_conf = {
    "bootstrap.servers": config.BOOTSTRAP_SERVERS,
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": config.KAFKA_API_KEY,
    "sasl.password": config.KAFKA_API_SECRET,
    "group.id": config.CONSUMER_GROUP,
    "auto.offset.reset": "latest",
}
# auto.offset.reset=latest: a live dashboard cares about current data, not a
# replay of every window since the table was created.


def consume_loop(topic: str, on_message):
    """Blocking loop — must run in its own thread, never on the FastAPI event loop."""
    consumer = Consumer(_consumer_conf)
    consumer.subscribe([topic])
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            value = _avro_deserializer(
                msg.value(), SerializationContext(topic, MessageField.VALUE)
            )
            on_message(value)
    finally:
        consumer.close()
