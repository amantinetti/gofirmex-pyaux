import json
import requests
from kafka import KafkaProducer
import time
from alive_progress import alive_bar
import uuid
import psycopg2


def extract_json(log_message: str) -> dict:
    # Find the starting index of json data
    start_idx = log_message.index('Error on process Message: value = ') + len('Error on process Message: value = ')
    # Cut the prefix part
    json_str = log_message[start_idx:]
    # Load json to Python dictionary
    data = json.loads(json_str)
    return data


def publish(producer, key, message, topic):
    ts = time.time()
    key = f"{key}_{ts}"
    # bmessage = json.dumps(message).encode('utf-8')
    producer.send(topic, key=key, value=message)
    producer.flush()


def get_waiting_to_publish(conn):
    sql = "select id from sign_documents where status_id = 6"
    cursor = conn.cursor()
    cursor.execute(sql, )
    documents = cursor.fetchall()
    cursor.close()

    return documents


if __name__ == '__main__':

    signing_conn = psycopg2.connect(database="signing_layer",
                                    host="10.2.0.2",
                                    user="signing_layer",
                                    password="kWn8W63Wxc6yJteoKEAhboNLiiMTMJ",
                                    port="5432", application_name="manti-python-script")

    documents_sended = []

    producer = KafkaProducer(
        bootstrap_servers='pkc-619z3.us-east1.gcp.confluent.cloud:9092',
        security_protocol='SASL_SSL',
        sasl_mechanism='PLAIN',
        sasl_plain_username='3TRSKIQOOTD7KKSX',
        sasl_plain_password='6/iRpFwakQsv2zhKsk8IECwx2tshzosH6hyCXDVHJ1k+0wsHL3g3rPp1AgD6LKmr',
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=str.encode
    )

    waiting_docs = get_waiting_to_publish(signing_conn)

    with alive_bar(len(waiting_docs), force_tty=True) as bar:
        for i, line in enumerate(waiting_docs):
            doc_id = line[0]

            message = {
                "signDocuments": [
                    {
                        "id": doc_id,
                        "registerRequired": False
                    }
                ]
            }

            publish(producer, doc_id, message, "signing.portfolio.receive.signing-provider-assignment")

            bar()
