import json
import requests
from kafka import KafkaProducer
import time
from alive_progress import alive_bar
import uuid


def extract_json(log_message: str) -> dict:
    # Find the starting index of json data
    start_idx = log_message.index('Error on process Message: value = ') + len('Error on process Message: value = ')
    # Cut the prefix part
    json_str = log_message[start_idx:]
    # Load json to Python dictionary
    data = json.loads(json_str)
    return data


def publish(producer, message, topic):
    ts = time.time()
    unique_id = uuid.uuid4()
    key = f"{unique_id}_{ts}"
    # bmessage = json.dumps(message).encode('utf-8')
    producer.send(topic, key=key, value=message)
    # producer.flush()


if __name__ == '__main__':
    jump_exists = True
    file = open('ms-ex-sign-esign.log', 'r')
    lines = file.readlines()

    documents_sended = []

    producer_cloud = KafkaProducer(
        bootstrap_servers='pkc-619z3.us-east1.gcp.confluent.cloud:9092',
        security_protocol='SASL_SSL',
        sasl_mechanism='PLAIN',
        sasl_plain_username='3TRSKIQOOTD7KKSX',
        sasl_plain_password='6/iRpFwakQsv2zhKsk8IECwx2tshzosH6hyCXDVHJ1k+0wsHL3g3rPp1AgD6LKmr',
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=str.encode
    )

    producer_local = KafkaProducer(
        bootstrap_servers='10.142.0.8:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=str.encode
    )

    producer = producer_local

    with alive_bar(len(lines), force_tty=True) as bar:
        for i, line in enumerate(lines):
            if "Error on process Message:" in line:
                data = line.split(", ")
                message = extract_json(data[0])
                topic = data[2].split(" ")[2]
                if message['document_id'] not in documents_sended:
                    print(f'send in {topic} message: {message}')
                    publish(producer, message, topic)
                    documents_sended.append(message['document_id'])
            bar()
