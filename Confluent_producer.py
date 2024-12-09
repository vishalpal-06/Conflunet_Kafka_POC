import cv2
from deepface import DeepFace

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer


def delivery_report(err, msg):
    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))

# Define Kafka configuration
kafka_config = {
    'bootstrap.servers': 'pkc-56d1g.eastus.azure.confluent.cloud:9092',
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': 'UPI43IFAOPTJ6PAN',
    'sasl.password': 'OyRHkfjrmKHID59bCnwsGYf+row92b3IVIxyKk10OmhVJa5ymIlPz8oUwNHrE1xy'
}

# Create a Schema Registry client
schema_registry_client = SchemaRegistryClient({
  'url': 'https://psrc-42jp1.westus2.azure.confluent.cloud',
  'basic.auth.user.info': '{}:{}'.format('6COL5NAXSJEPTRFE', 'gu5X7RwfLNw3SvuUBBwSIH0T2YesaEa3WFnCJ3GnEczrmILAHkpMrXyi+8/VCnrx')
})

# Fetch the latest Avro schema for the value
subject_name = 'topic_0-value'
schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str

# Create Avro Serializer for the value
# schema_str='{"type": "string"}'
key_serializer = StringSerializer('utf_8')
avro_serializer = AvroSerializer(schema_registry_client, schema_str)

# Define the SerializingProducer
producer = SerializingProducer({
    'bootstrap.servers': kafka_config['bootstrap.servers'],
    'security.protocol': kafka_config['security.protocol'],
    'sasl.mechanisms': kafka_config['sasl.mechanisms'],
    'sasl.username': kafka_config['sasl.username'],
    'sasl.password': kafka_config['sasl.password'],
    'key.serializer': key_serializer,  # Key will be serialized as a string
    'value.serializer': avro_serializer  # Value will be serialized as Avro
})


def analyze_image(frame):
    analysis = DeepFace.analyze(frame, actions=['emotion', 'age', 'gender'], enforce_detection=False)
    for result in analysis:
        Emotion = result['dominant_emotion']
        Age = result['age']
        Gender = 'Woman' if result['gender'] == 'Woman' else 'Man'

        my_dict = {'Emotion':Emotion,'Age':Age,"Gender":Gender}
    return my_dict

def capture_from_webcam():
    cap = cv2.VideoCapture(0)
    if not cap.isOpened():
        print("Error: Could not open webcam.")
        return
    id = 1
    
    while True:
        ret, frame = cap.read()
        if not ret:
            print("Error: Could not read frame.")
            break
        return_dic = analyze_image(frame)

        producer.produce(topic='topic_0', key=str(id), value=return_dic, on_delivery=delivery_report)
        producer.flush()

        id = id + 1
        cv2.imshow('Webcam', frame)
        if cv2.waitKey(1) & 0xFF == ord('q'):
            break

    cap.release()
    cv2.destroyAllWindows()


capture_from_webcam()