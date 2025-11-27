import base64
import requests
import json
import psycopg2
import urllib.request
from alive_progress import alive_bar
from os.path import exists
import csv
import hashlib

files_manager_url = "https://files-manager.cloud-run.fexcloud.io"


def shorten_uuid(original_uuid):
    # Convertir el UUID (string) a bytes
    uuid_bytes = original_uuid.encode('utf-8')
    # Crear un hash con SHA-256 del UUID
    hash_object = hashlib.sha256(uuid_bytes)
    # Convertir el hash a Base64 para acortarlo
    hash_base64 = base64.urlsafe_b64encode(hash_object.digest()).decode('utf-8')
    # Devolver solo los primeros caracteres para acortar (opcional: ajustar longitud según necesidad)
    return hash_base64[:22]  # Configurar longitud para uso específico


def get_pending_documents(conn, notary_id):
    sql = """ select sign_documents.id
              from sign_documents
                       join portfolios on sign_documents.portfolio_id = portfolios.id
                       join document_types on sign_documents.type_id = document_types.id
              where status_id = 7
                and (portfolios.notary_id = %s or sign_documents.notary_id = %s)
                and notary_signed_at is null """
    cursor = conn.cursor()
    cursor.execute(sql, (notary_id, notary_id,))
    documents = cursor.fetchall()
    cursor.close()

    return documents


def download_file(document_id, filename, doc_type="notary_document"):
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
    urllib.request.urlretrieve(data['document_url'], filename)
    return filename


if __name__ == '__main__':
    notary_conn = psycopg2.connect(database="notary_layer",
                                   host="10.2.0.2",
                                   user="notary_layer",
                                   password="YsB7cV9LWWDA4LenaGDsCCRz06fevi",
                                   port="5432",
                                   application_name="manti-python-script")

    # notary = "80cb094c-f3f5-494a-81e3-677505da48f1" # 42°NOTARIA DE SANTIAGO ALVARO DAVID GONZALEZ SALINAS
    # short_name = False

    #notary = "0eef8612-a840-4d06-9821-ce49278f8089"  # Notaria 1° NOTARIA DE INDEPENDENCIA
    notary = "0d265acb-e618-408b-aa58-801a2f4a0889"  # Notaria 2° Notaría de San Miguel Fabián Díaz Contreras
    short_name = True

    jump_exists = True
    jump_in_downloaded = True

    documents = get_pending_documents(notary_conn, notary)

    already_downloaded = []
    already_downloaded_file = open('downloaded_docs', 'r')
    lines = already_downloaded_file.readlines()
    for line in lines:
        already_downloaded.append(line.strip().lower())
    already_downloaded_file.close()

    downloads = open('downloaded_docs', 'a')

    downloaded = 0
    jump_in_downloaded_count = 0

    with alive_bar(len(documents), force_tty=True) as bar:
        for document in documents:
            if jump_in_downloaded:
                if document[0].lower() in already_downloaded:
                    print("Jump already downloaded file {}".format(document[0]))
                    jump_in_downloaded_count += 1
                    bar()
                    continue

            filename = f'docs/FDI-{document[0].upper()}.pdf'
            if short_name:
                filename = f'docs/FD-{shorten_uuid(document[0].upper())}.pdf'
            if jump_exists:
                if exists(filename):
                    print("Jump already exist file {}".format(document[0]))
                    bar()
                    continue
            filename = download_file(document[0], filename)
            print("Document ID -> {} Filename -> {}".format(document[0], filename))
            downloads.write(document[0] + "\n")
            downloads.flush()
            downloaded += 1
            bar()

    downloads.close()
    notary_conn.close()

    print("Downloaded {} documents".format(downloaded))
    print("Already Downloaded But Not signed {} documents".format(jump_in_downloaded_count))
