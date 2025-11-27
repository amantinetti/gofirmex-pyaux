import base64
import requests
import json
import psycopg2
import urllib.request
from alive_progress import alive_bar
from os.path import exists
import csv

from exceptiongroup import catch

from Notary.upload_from_ajs import analyze_pdf, AnalyzeException

files_manager_url = "https://files-manager.cloud-run.gofirmex.cloud"


def get_file_url(document_id, doc_type="notary_document"):
    url = f'{files_manager_url}/documents/download_last_by_external_id'

    payload = json.dumps({
        "external_document_id": document_id,
        "external_document_type": doc_type,  ##"sign_document",
        "extended_expiration": True
    })
    headers = {
        'Content-Type': 'application/json'
    }

    response = requests.request("POST", url, headers=headers, data=payload)

    data = response.json()
    return data['document_url']


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
    filename = f'docs/FND-{document_id.upper()}.pdf'
    urllib.request.urlretrieve(data['documents'][docsLen - 2]['url'], filename)
    return filename


if __name__ == '__main__':
    notary_conn = psycopg2.connect(database="notary_layer",
                                   host="10.2.0.2",
                                   user="notary_layer",
                                   password="YsB7cV9LWWDA4LenaGDsCCRz06fevi",
                                   port="5432", application_name="manti-python-script")

    file = open('casos_mal_cargados', 'r')
    lines = file.readlines()

    file_write = open('redownload_docs', 'w')

    with alive_bar(len(lines), force_tty=True) as bar:
        for i, line in enumerate(lines):
            document_id = line.strip().lower()

            file_url = get_file_url(document_id, 'sign_document')
            # analysis_status = False
            try:
                analysis_status = analyze_pdf(document_id, file_url)
            except AnalyzeException as e:
                print("Caso a revisar ID {} Exception -> {}".format(document_id, str(e)))
                file_write.write(document_id + "\n")

            # if not analysis_status:
            #    print("Caso a revisar ID {}".format(document_id))
            #    file_write.write(document_id + "\n")

            bar()

    file_write.close()
    notary_conn.close()
