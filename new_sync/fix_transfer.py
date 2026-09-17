import base64
import requests
import json
import psycopg2
import urllib.request
from alive_progress import alive_bar
from os.path import exists
import csv
from kafka import KafkaProducer
import time


def fix(conn, sign_document_id, signer_id):
    sql = "INSERT INTO public.sign_document_signers (sign_document_id, signer_id) VALUES (%s, %s)"
    cursor = conn.cursor()
    cursor.execute(sql, (sign_document_id, signer_id,))
    conn.commit()
    cursor.close()


def check(conn, sign_document_id, signer_id):
    sql = "select sign_document_id, signer_id from \"sign_document_signers\" where sign_document_id = %s and signer_id = %s"
    cursor = conn.cursor()
    cursor.execute(sql, (sign_document_id, signer_id,))
    data = cursor.fetchall()
    cursor.close()
    return data


def get_sign_layer(conn, sign_document_id):
    sql = "select sign_document_id, signer_id from \"sign_document_signers\" where sign_document_id = %s"
    cursor = conn.cursor()
    cursor.execute(sql, (sign_document_id,))
    data = cursor.fetchall()
    cursor.close()

    return data


def publish(producer, key, message, topic):
    ts = time.time()
    key = f"{key}_{ts}"
    # bmessage = json.dumps(message).encode('utf-8')
    producer.send(topic, key=key, value=message)
    producer.flush()


if __name__ == '__main__':

    retry = True

    producer = KafkaProducer(
        bootstrap_servers='10.142.0.8:9092',
        # security_protocol='SASL_SSL',
        # sasl_mechanism='PLAIN',
        # sasl_plain_username='3TRSKIQOOTD7KKSX',
        # sasl_plain_password='6/iRpFwakQsv2zhKsk8IECwx2tshzosH6hyCXDVHJ1k+0wsHL3g3rPp1AgD6LKmr',
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=str.encode
    )

    signing_conn = psycopg2.connect(database="signing_layer",
                                    host="10.2.0.2",
                                    user="signing_layer",
                                    password="kWn8W63Wxc6yJteoKEAhboNLiiMTMJ",
                                    port="5432", application_name="manti-python-script")

    provider = psycopg2.connect(database="signing_provider",
                                host="10.2.0.2",
                                user="signing_provider",
                                password="WMXxD2VhfK5mx7F5iWG8eKCQvMAMhB",
                                port="5432", application_name="manti-python-script")

    data = ["be502b56-c97f-49f7-91d5-220c671be357",
            "af62c01e-2073-4b2d-8707-9633928464ce"]

    with alive_bar(len(data), force_tty=True) as bar:
        for sign_document_id in data:
            signers = get_sign_layer(signing_conn, sign_document_id)
            for signer in signers:
                signer_id = signer[1]
                d = check(provider, sign_document_id, signer_id)
                if len(d) == 0:
                    fix(provider, sign_document_id, signer_id)

            message = {
                "sign_document_id": sign_document_id,
                "publication_status": 1,
                "sign_by_portfolio": True
            }

            publish(producer, signer_id, message, "signing.sign-document.receive.publish")

            bar()

    signing_conn.close()
    provider.close()
