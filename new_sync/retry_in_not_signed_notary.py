import base64
import requests
import json
import psycopg2
import urllib.request
from alive_progress import alive_bar
from os.path import exists
import csv


def get_documents_in_portfolio(conn, portfolio_id):
    sql = "select id from sign_documents where portfolio_id = %s"
    cursor = conn.cursor()
    cursor.execute(sql, (portfolio_id,))
    documents = cursor.fetchall()
    cursor.close()

    return documents


def retry_printing(document_id):
    url = "http://10.142.0.16/ms/notary-workflow-manager/v1/workflow-utilities/retry/sign-document/notary-printing/" + document_id  # Retry
    # url = "http://10.142.0.16/ms/notary-workflow-manager/v1/workflow-utilities/reprocess/retry/sign-document/notary-printing/" + document_id  # Force
    response = requests.request("PUT", url)
    print(f"POST -> {url} Response Status -> {response.status_code} Text -> {response.text}")


def reprocess_portfolio(document_id):
    url = "http://10.142.0.16/ms/workflow-utilities/v1/reset/notary-workflow/portfolio/" + document_id  # Retry
    response = requests.request("PUT", url)
    print(f"POST -> {url} Response Status -> {response.status_code} Text -> {response.text}")


if __name__ == '__main__':

    retry = True

    notary_conn = psycopg2.connect(database="notary_layer",
                                   host="10.2.0.2",
                                   user="notary_layer",
                                   password="YsB7cV9LWWDA4LenaGDsCCRz06fevi",
                                   port="5432", application_name="manti-python-script")

    with open('not_signed_notary.csv', 'r', newline='') as csvfile:
        # Crea un lector CSV que leerá el archivo con encabezados
        csvreader = csv.DictReader(csvfile)

        data = list(csvreader)

        # Itera sobre las filas del archivo CSV
        with alive_bar(len(data), force_tty=True) as bar:
            for row in data:
                if row['problem'] == "pegado en notary printing":
                    documents = get_documents_in_portfolio(notary_conn, row['portfolio_id'])
                    for document in documents:
                        print(
                            f"Print Portfolio ID: {row['portfolio_id']}, Problem: {row['problem']}, Last Update: {row['last_update']}, Document ID: {document[0]}")
                        retry_printing(document[0])
                elif row['problem'] == "sin signacion de notario y con status en notaria" or row['problem'] == "pendiente de asignacion de notaria":
                    print(
                        f"Reprocess Portfolio ID: {row['portfolio_id']}, Problem: {row['problem']}, Last Update: {row['last_update']}")
                    reprocess_portfolio(row['portfolio_id'])
                bar()

    notary_conn.close()
