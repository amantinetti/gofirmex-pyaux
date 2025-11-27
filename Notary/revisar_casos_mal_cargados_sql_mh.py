import base64
import requests
import json
import psycopg2
import urllib.request
from alive_progress import alive_bar
from os.path import exists
import csv
from Notary.upload_from_ajs import analyze_pdf, AnalyzeException

from concurrent.futures import ThreadPoolExecutor
from threading import Lock
from alive_progress import alive_bar

# Bloqueo para sincronizar el acceso a la escritura en el archivo
file_write_lock = Lock()

files_manager_url = "https://files-manager.cloud-run.gofirmex.cloud"

exclude = []


def get_file_url(document_id, doc_type="sign_document"):
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


def download_file(document_id, doc_type="sign_document"):
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


def get_signed_documents(conn, notary_id):
    sql = """select sign_documents.id
        from sign_documents
                 join portfolios on sign_documents.portfolio_id = portfolios.id
        where portfolios.notary_id = %s
          and portfolios.finished_at >= '2025-03-12 00:00:00'
          and portfolios.status_id = 13
          and sign_documents.notary_signing_type_id is not null
        order by portfolios.finished_at"""
    cursor = conn.cursor()
    cursor.execute(sql, (notary_id,))
    documents = cursor.fetchall()
    cursor.close()

    return documents


def process_document_thread_safe(document_id, file_write):
    try:
        file_url = get_file_url(document_id, 'sign_document')

        try:
            analysis_status = analyze_pdf(document_id, file_url)
        except AnalyzeException as e:
            with file_write_lock:  # Asegura que sólo un hilo escribe al archivo a la vez
                print("Caso a revisar ID {} Exception -> {}".format(document_id, str(e)))
                file_write.write(document_id + "\n")

        # analysis_status = analyze_pdf(document_id, file_url)
        # if not analysis_status:
        #    with file_write_lock:  # Asegura que sólo un hilo escribe al archivo a la vez
        #        print("Caso a revisar ID {}".format(document_id))
        #        file_write.write(document_id + "\n")
    except Exception as e:
        print(f"Error procesando el documento {document_id}: {e}")


if __name__ == '__main__':
    notary_conn = psycopg2.connect(database="notary_layer",
                                   host="10.2.0.2",
                                   user="notary_layer",
                                   password="YsB7cV9LWWDA4LenaGDsCCRz06fevi",
                                   port="5432", application_name="manti-python-script")

    documental_conn = psycopg2.connect(database="documental_layer",
                                       host="10.2.0.2",
                                       user="documental_layer",
                                       password="vKA5h2CZmf1p6EgQKBPRn8YpZDTmh2ay",
                                       port="5432", application_name="manti-python-script")

    file_read = open('redownload_docs', 'r')
    lines = file_read.readlines()
    for line in file_read:
        exclude.append(line.strip())
    file_read.close()

    file_write = open('redownload_docs', 'a')

    # notary_id = '0eef8612-a840-4d06-9821-ce49278f8089' ## Notaria Independencia
    notary_id = '80cb094c-f3f5-494a-81e3-677505da48f1'  ## Notaria 42 Gonzalez
    check_documents = get_signed_documents(documental_conn, notary_id)

    documents = []

    for i, sql_line in enumerate(check_documents):
        document_id = sql_line[0]

        if document_id in exclude:
            continue
        documents.append(document_id)

    with alive_bar(len(documents), force_tty=True) as bar:
        with ThreadPoolExecutor(max_workers=10) as executor:
            # Procesar documentos en hilos
            futures = [executor.submit(process_document_thread_safe, document_id, file_write) for document_id in
                       documents]
            for future in futures:
                future.result()  # Para manejar excepciones si ocurren
                bar()

    documental_conn.close()
    notary_conn.close()
    file_write.close()
