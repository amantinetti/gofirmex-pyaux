import base64
import requests
import json
import psycopg2
import urllib.request
from alive_progress import alive_bar
from os.path import exists
import csv


def get_in_sign_documents(conn):
    sql = "select id from sign_documents where status_id = 7"
    cursor = conn.cursor()
    cursor.execute(sql, )
    portfolios = cursor.fetchall()
    cursor.close()

    return portfolios


def get_document_status_in_signing(conn, portfolio_id):
    sql = "select status_id from sign_documents where id = %s"
    cursor = conn.cursor()
    cursor.execute(sql, (portfolio_id,))
    data = cursor.fetchone()
    cursor.close()

    return data[0]


if __name__ == '__main__':
    esign_conn = psycopg2.connect(database="ex_sign_esign",
                                  host="10.2.0.2",
                                  user="ex_sign_esign",
                                  password="Xkt0WkVxKj23noXxRjnsnnsQN3JLad",
                                  port="5432", application_name="manti-python-script")

    signing_conn = psycopg2.connect(database="signing_layer",
                                    host="10.2.0.2",
                                    user="signing_layer",
                                    password="kWn8W63Wxc6yJteoKEAhboNLiiMTMJ",
                                    port="5432", application_name="manti-python-script")

    signed_documents = get_finished_document_in_esign(esign_conn)

    with open('not_signed_document.csv', 'w', newline='') as csvfile:

        spamwriter = csv.writer(csvfile, delimiter=',',
                                quotechar='|', quoting=csv.QUOTE_MINIMAL)
        spamwriter.writerow(['document_id'])
        csvfile.flush()

        with alive_bar(len(signed_documents), force_tty=True) as bar:
            for document in signed_documents:
                document_id = document[0]
                status = get_document_status_in_signing(signing_conn, document_id)
                if status == 7:  # estatus pendiente de firmas
                    spamwriter.writerow([document_id])
                    csvfile.flush()
                bar()

    esign_conn.close()
    signing_conn.close()
