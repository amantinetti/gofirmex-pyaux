import base64
import requests
import json
import psycopg2
import urllib.request
from alive_progress import alive_bar
from os.path import exists
import csv
from Notary.upload_from_ajs import analyze_pdf

from concurrent.futures import ThreadPoolExecutor
from threading import Lock
import os

files_manager_url = "https://files-manager.cloud-run.gofirmex.cloud"

# Bloqueo para sincronizar el acceso a la escritura en el archivo
file_write_lock = Lock()


def create_downloaded_docs_folder(folder_name='downloaded_docs'):
    # Obtiene la carpeta de Descargas del usuario
    downloads_folder = os.path.expanduser("~/Downloads")

    # Define el nombre de la nueva carpeta
    folder_path = os.path.join(downloads_folder, folder_name)

    # Crea la carpeta si no existe
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
        print(f"Carpeta creada exitosamente en: {folder_path}")
    else:
        print(f"La carpeta ya existe en: {folder_path}")

    return folder_path


def get_finished_documents(conn, client_id):
    sql = """ 
            select sign_documents.id
        from sign_documents
            join sign_document_statuses on sign_documents.status_id = sign_document_statuses.id
                 join portfolios on sign_documents.portfolio_id = portfolios.id
                 join portfolio_statuses on portfolios.status_id = portfolio_statuses.id
        where portfolio_statuses.name = 'SUCCESS'
        and sign_document_statuses.name = 'SUCCESS'
        and portfolios.client_id = %s
        order by portfolios.finished_at
        """
    cursor = conn.cursor()
    cursor.execute(sql, (client_id,))
    documents = cursor.fetchall()
    cursor.close()

    return documents


def download_file(document_id, folder, doc_type="sign_document"):
    url = f'{files_manager_url}/documents/download_last_by_external_id'

    payload = json.dumps({
        "external_document_id": document_id,
        "external_document_type": doc_type,
        "extended_expiration": True
    })
    headers = {
        'Content-Type': 'application/json'
    }

    response = requests.request("POST", url, headers=headers, data=payload)

    data = response.json()
    filename = f'{folder}/{document_id.upper()}.pdf'
    urllib.request.urlretrieve(data['document_url'], filename)
    return filename


def process_document_thread_safe(document_id, jump_exists, folder):
    try:
        if jump_exists:
            filename = f'{folder}/{document_id.upper()}.pdf'
            if exists(filename):
                with file_write_lock:
                    print("Jump already exist file {}".format(document_id))
                return
        filename = download_file(document_id, folder)
        with file_write_lock:
            print("Document ID -> {} Filename -> {}".format(document_id, filename))

    except Exception as e:
        print(f"Error procesando el documento {document_id}: {e}")


if __name__ == '__main__':
    documental_conn = psycopg2.connect(database="documental_layer",
                                       host="10.2.0.2",
                                       user="documental_layer",
                                       password="vKA5h2CZmf1p6EgQKBPRn8YpZDTmh2ay",
                                       port="5432", application_name="manti-python-script")

    client_id = "63852b52-1156-4734-be1c-789bc05fca96"  # U Austral
    jump_exists = True
    documents = get_finished_documents(documental_conn, client_id)

    documental_conn.close()
    downloaded = 0

    folder = create_downloaded_docs_folder('uaustral_docs')

    with alive_bar(len(documents), force_tty=True) as bar:
        with ThreadPoolExecutor(max_workers=20) as executor:
            # Procesar documentos en hilos
            futures = [executor.submit(process_document_thread_safe, document_id[0], jump_exists, folder) for
                       document_id in
                       documents]
            for future in futures:
                future.result()  # Para manejar excepciones si ocurren
                bar()

    # with alive_bar(len(documents), force_tty=True) as bar:
    #     for document in documents:
    #         if jump_exists:
    #             filename = f'downloaded_docs/{document[0].upper()}.pdf'
    #             if exists(filename):
    #                 print("Jump already exist file {}".format(document[0]))
    #                 bar()
    #                 continue
    #         filename = download_file(document[0])
    #         print("Document ID -> {} Filename -> {}".format(document[0], filename))
    #         downloaded += 1
    #         bar()
    # print("Downloaded {} documents".format(downloaded))
