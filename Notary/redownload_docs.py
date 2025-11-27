import base64
import requests
import json
import psycopg2
import urllib.request
from alive_progress import alive_bar
from os.path import exists
import csv

files_manager_url = "https://files-manager.cloud-run.gofirmex.cloud"


def download_file(document_id, doc_type="notary_document"):
    url = f'{files_manager_url}/documents/history_by_external_id'

    payload = json.dumps({
        "external_document_id": document_id,
        "external_document_type": doc_type,  ##"sign_document",
        "get_urls": True
    })
    headers = {
        'Content-Type': 'application/json'
    }

    response = requests.request("POST", url, headers=headers, data=payload)

    data = response.json()

    docsLen = len(data['documents'])
    filename = f'docs/FNDP-{document_id.upper()}.pdf'
    urllib.request.urlretrieve(data['documents'][docsLen - 2]['url'], filename)
    return filename


if __name__ == '__main__':
    notary_conn = psycopg2.connect(database="notary_layer",
                                   host="10.2.0.2",
                                   user="notary_layer",
                                   password="YsB7cV9LWWDA4LenaGDsCCRz06fevi",
                                   port="5432", application_name="manti-python-script")

    file = open('redownload_docs', 'r')
    lines = file.readlines()

    with alive_bar(len(lines), force_tty=True) as bar:
        for i, line in enumerate(lines):
            document_id = line.strip().lower()

            filename = f'docs/FNDP-{document_id.upper()}.pdf'
            if exists(filename):
                print("Jump already exist file {}".format(document_id))
                bar()
                continue

            download_file(document_id, "notary_document")
            print("Document ID -> {} Filename -> {}".format(document_id, filename))
            bar()

    notary_conn.close()
