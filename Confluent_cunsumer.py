from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer

import csv
import os


SERVER = os.getenv("SERVER")
USERNAME = os.getenv("USERNAME")
PASS = os.getenv("PASS")
ENDPOINT = os.getenv("ENDPOINT")
SCHEMA_REGISTORY_API = os.getenv("SCHEMA_REGISTORY_API")
SCHEMA_REGISTORY_SECRET = os.getenv("SCHEMA_REGISTORY_SECRET")

# Define Kafka configuration
kafka_config = {
    'bootstrap.servers': SERVER,
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': USERNAME,
    'sasl.password': PASS,
    'group.id': 'group1',
    'auto.offset.reset': 'earliest'
}


# Create a Schema Registry client
schema_registry_client = SchemaRegistryClient({
  'url': ENDPOINT,
  'basic.auth.user.info': '{}:{}'.format(SCHEMA_REGISTORY_API, SCHEMA_REGISTORY_SECRET)
})

# Fetch the latest Avro schema for the value
subject_name = 'topic_0-value'
schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str

# Create Avro Deserializer for the value
key_deserializer = StringDeserializer('utf_8')
avro_deserializer = AvroDeserializer(schema_registry_client, schema_str)

# Define the DeserializingConsumer
consumer = DeserializingConsumer({
    'bootstrap.servers': kafka_config['bootstrap.servers'],
    'security.protocol': kafka_config['security.protocol'],
    'sasl.mechanisms': kafka_config['sasl.mechanisms'],
    'sasl.username': kafka_config['sasl.username'],
    'sasl.password': kafka_config['sasl.password'],
    'key.deserializer': key_deserializer,
    'value.deserializer': avro_deserializer,
    'group.id': kafka_config['group.id'],
    'auto.offset.reset': kafka_config['auto.offset.reset']
})

# Subscribe to the 'topic_0' topic
consumer.subscribe(['topic_0'])

#Continually read messages from Kafka
try:
    while True:
        msg = consumer.poll(10.0) # How many seconds to wait for message
        if msg is None:
            continue
        if msg.error():
            print('Consumer error: {}'.format(msg.error()))
            continue

        dic_res = msg.value()
        dic_res['id'] = int(msg.key())
        
        with open('getted_data.csv','a', newline='') as csvfile:
            fieldnames = dic_res.keys()
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            check_file = os.stat("getted_data.csv").st_size
            if(check_file == 0):
                writer.writeheader()
                writer.writerow(dict(zip(fieldnames, 'header_row')))
            writer.writerow(dic_res)

except KeyboardInterrupt:
    pass
finally:
    consumer.close()