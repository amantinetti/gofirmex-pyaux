from os.path import exists

import psycopg2
import json
import requests
import urllib.request

from alive_progress import alive_bar

import new_api.service
from pathlib import Path
import csv


signersAssetplan = [{
    "nin": "14119304-K",
    "names": "JOSÉ IGNACIO",
    "lastnames": "TORRETTI SCHMIDT",
    "email": "ignacio.torretti@assetplan.cl",
    # "phone": "+56972824218",
    "notification": "none",
    "country": "CL",
    # "front": "./documents/3107709_front.png",
    # "back": "./documents/3107709_back.png"
}]

customerID = "1ef952d0-0a0f-4b6c-b76f-1b75b7b90f3b"  ## Assetplan Asesores SpA


files_manager_url = "https://files-manager.cloud-run.fexcloud.io"


def download_file(document_id, filename, doc_type="sign_document"):
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


def get_documents(portfolio_id, conn):
    sql = "select id, custom_id, description, type_id from sign_documents where portfolio_id = %s"
    cursor = conn.cursor()
    cursor.execute(sql, (portfolio_id,))
    documents = cursor.fetchall()
    cursor.close()
    return documents



if __name__ == '__main__':
    conn = psycopg2.connect(database="documental_layer",
                            host="10.2.0.2",
                            user="documental_layer",
                            password="vKA5h2CZmf1p6EgQKBPRn8YpZDTmh2ay",
                            port="5432", application_name="manti-python-script")

    #download_folder = "./massive_assetplan_downloads"
    #Path(download_folder).mkdir(parents=True, exist_ok=True)

    process_folder = "./proccessed_assetplan_downloads"
    Path(process_folder).mkdir(parents=True, exist_ok=True)

    input_csv = "massive_download.csv"
    output_csv = "assetplan_clone_result.csv"

    # Leemos primero todos los portfolio_id (sin borrar el archivo de entrada)
    with open(input_csv, "r", newline="", encoding="utf-8") as infile:
        reader = csv.reader(infile)
        rows = [row for row in reader if row and row[0].strip()]

    # Saltamos el header si existe
    if rows and rows[0][0].strip().lower() == "portfolio_id":
        rows = rows[1:]

    olds_portfolios_ids = []
    for row in rows:
        olds_portfolios_ids.append(row[0].strip())

    total = len(olds_portfolios_ids)

    if total < len(rows):
        print("Existen portafolios duplicados en la lista")

    # Leemos el output_csv previo (si existe) para saltar portafolios ya procesados
    processed = set()
    if exists(output_csv):
        with open(output_csv, "r", newline="", encoding="utf-8") as done_file:
            done_reader = csv.reader(done_file)
            for done_row in done_reader:
                if not done_row or not done_row[0].strip():
                    continue
                if done_row[0].strip().lower() == "portfolio_id":
                    continue
                processed.add(done_row[0].strip())

    # Escribimos el resultado incrementalmente (append) para no perder progreso previo
    output_exists = exists(output_csv)
    with open(output_csv, "a", newline="", encoding="utf-8") as outfile:
        writer = csv.writer(outfile)
        if not output_exists:
            writer.writerow(["portfolio_id", "documents", "new_portfolio_id"])

        with alive_bar(total, force_tty=True) as bar:
            for idx, old_portfolio_id in enumerate(olds_portfolios_ids, start=1):

                if old_portfolio_id in processed:
                    print(f"({idx}/{total}) {old_portfolio_id} -> ya procesado, se salta")
                    bar(skipped=True)
                    continue

                old_documents = get_documents(old_portfolio_id, conn)

                new_documents = []

                for i, old_document in enumerate(old_documents):
                    document_id = old_document[0]
                    custom_id = old_document[1]
                    description = old_document[2]
                    type_id = old_document[3]
                    filename = "{}/{}.pdf".format(process_folder, document_id)
                    if not exists(filename):
                        print(f"  [ERROR] documento {document_id} de {old_portfolio_id}")
                        exit(1)
                    new_documents.append(
                        {
                            "file_route": filename,
                            "client_document_id": custom_id,
                            "description": description,
                            "document_type": type_id,
                            "signature": "FEA",
                        }
                    )

                new_portfolio_id = new_api.service.create_full_portfolio(new_documents, [], signersAssetplan, customerID, True)

                if new_portfolio_id == None:
                    print(f"  [ERROR] no se pudo crear el portfolio {old_portfolio_id}")
                    exit(1)

                writer.writerow([old_portfolio_id, len(new_documents), new_portfolio_id])
                outfile.flush()
                print(f"({idx}/{total}) {old_portfolio_id} -> {len(new_documents)} documentos -> {new_portfolio_id}")
                bar()

    conn.close()
    print(f"Listo. Resultados en {output_csv}")