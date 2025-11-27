import base64
import requests
import json
import psycopg2
import urllib.request
from alive_progress import alive_bar
from os.path import exists
import csv
import PyPDF2
import re
import io

from Notary.download import shorten_uuid

ajs_url = "https://repositorio.registrosnotariales.cl:8181/rest/api/verificar/"


class AnalyzeException(Exception):
    def __init__(self, message: str, document_id=None):
        """
        Constructor de la excepción AnalyzeException.
        :param message: Mensaje de error.
        :param document_id: (Opcional) ID del documento relacionado con el error.
        """
        self.document_id = document_id
        super().__init__(f"AnalyzeException: {message} {f'(Document ID: {document_id})' if document_id else ''}")


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


def download_file(document_notary_id):
    url = f'{ajs_url}{document_notary_id}'
    response = requests.request("GET", url)
    data = response.json()
    if 'url' in data:
        return data['url']
    return ''


def analyze_pdf(document_id, file_url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Windows; Windows x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.5060.114 Safari/537.36'}
    response = requests.get(url=file_url, headers=headers, timeout=120)
    on_fly_mem_obj = io.BytesIO(response.content)

    reader = PyPDF2.PdfReader(on_fly_mem_obj)
    num_pages = len(reader.pages)

    string = "{}.pdf".format(document_id)

    page = reader.pages[num_pages - 2]
    text = page.extract_text()
    res_search = re.search(string, text)

    if not res_search:
        raise AnalyzeException('Document ID Not Found')

#    if not reader.is_encrypted:
#        raise AnalyzeException('Document Signs Not Found')

    return True


def upload_to_notary(document_id, file_url):
    url = "http://10.142.0.16/ms/notary-workflow-manager/v1/notary-portal/sign-document-sign"
    #url = "http://localhost:3000/notary-portal/sign-document-sign"

    payload = json.dumps({
        "signDocumentId": document_id,
        "fileUrl": file_url,
        # "notaryID": "cca15b0d-2fd5-45a1-929e-229e8aa7024d"
        "ignoreDocumentValidation": True,
    })
    headers = {
        'Content-Type': 'application/json'
    }

    response = requests.request("PUT", url, headers=headers, data=payload)
    if response.status_code != 200 and response.status_code != 201:
        print("ERROR ON UPLOAD DOCUMENT")
        print(response.text)
        return False
    return True


if __name__ == '__main__':
    notary_conn = psycopg2.connect(database="notary_layer",
                                   host="10.2.0.2",
                                   user="notary_layer",
                                   password="YsB7cV9LWWDA4LenaGDsCCRz06fevi",
                                   port="5432", application_name="manti-python-script")

    # notary = "80cb094c-f3f5-494a-81e3-677505da48f1" # 42°NOTARIA DE SANTIAGO ALVARO DAVID GONZALEZ SALINAS

    notary = "0eef8612-a840-4d06-9821-ce49278f8089"  # Notaria 1° NOTARIA DE INDEPENDENCIA
    try_recover = False

    documents = get_pending_documents(notary_conn, notary)

    with alive_bar(len(documents), force_tty=True) as bar:
        for document in documents:
            document_id = document[0]

            possible_names = [
                "039-FD-{}".format(shorten_uuid(document_id.upper())),
                "039-FD-{}".format(shorten_uuid(document_id.lower())),
                #"039-FDI-{}".format(document_id),
                #"039-FND-{}".format(document_id),

            ]
            for item in possible_names:
                file_url = download_file(item)
                if file_url != '':
                    same_document = analyze_pdf(document_id, file_url)
                    if same_document:
                        upload_result = upload_to_notary(document[0], file_url)
                        print("Document ID -> {} Upload Status -> {}".format(document[0], upload_result))
                    else:
                        print("Document ID -> {} No Correspond URL -> {}".format(document[0], file_url))

            bar()

    notary_conn.close()
