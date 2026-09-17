import base64
import requests
import json
import psycopg2
import urllib.request
from alive_progress import alive_bar
from os.path import exists
import csv


def get_pending_documents(conn, notary_id):
    sql = """ select sign_documents.id
        from sign_documents
                 join portfolios on sign_documents.portfolio_id = portfolios.id
                 join document_types on sign_documents.type_id = document_types.id
        where status_id = 7
          and notary_id = %s
          and notary_signed_at is null """
    cursor = conn.cursor()
    cursor.execute(sql, (notary_id,))
    documents = cursor.fetchall()
    cursor.close()

    return documents


def retry_printing(document_id):
    #url = "http://10.142.0.16/ms/notary-workflow-manager/v1/workflow-utilities/retry/sign-document/notary-printing/" + document_id  # Retry
    url = "http://10.142.0.16/ms/notary-workflow-manager/v1/workflow-utilities/reprocess/retry/sign-document/notary-printing/" + document_id  # Force
    response = requests.request("PUT", url)
    print(f"POST -> {url} Response Status -> {response.status_code} Text -> {response.text}")


if __name__ == '__main__':

    retry = True
    notary = "0eef8612-a840-4d06-9821-ce49278f8089"  # Notaria 1° NOTARIA DE INDEPENDENCIA

    notary_conn = psycopg2.connect(database="notary_layer",
                                   host="10.2.0.2",
                                   user="notary_layer",
                                   password="YsB7cV9LWWDA4LenaGDsCCRz06fevi",
                                   port="5432", application_name="manti-python-script")

    documents = get_pending_documents(notary_conn, notary)

    with alive_bar(len(documents), force_tty=True) as bar:
        for document in documents:
            retry_printing(document[0])
            bar()

    notary_conn.close()
