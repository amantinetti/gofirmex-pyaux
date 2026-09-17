from os.path import exists

import psycopg2

import new_api.service
from pathlib import Path
import csv
import json
import requests
import urllib.request
from alive_progress import alive_bar

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
    sql = "select id from sign_documents where portfolio_id = %s"
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

    carpeta = "./massive_assetplan_downloads"
    Path(carpeta).mkdir(parents=True, exist_ok=True)

    input_csv = "massive_download.csv"
    output_csv = "massive_download_result.csv"

    # Leemos primero todos los portfolio_id (sin borrar el archivo de entrada)
    with open(input_csv, "r", newline="", encoding="utf-8") as infile:
        reader = csv.reader(infile)
        rows = [row for row in reader if row and row[0].strip()]

    # Saltamos el header si existe
    if rows and rows[0][0].strip().lower() == "portfolio_id":
        rows = rows[1:]

    total = len(rows)

    # Escribimos el resultado incrementalmente para no perder progreso si algo falla
    with open(output_csv, "w", newline="", encoding="utf-8") as outfile:
        writer = csv.writer(outfile)
        writer.writerow(["portfolio_id", "downloaded_count"])

        with alive_bar(total, force_tty=True) as bar:
            for idx, row in enumerate(rows, start=1):
                portfolio_id = row[0].strip()
                downloaded = 0

                try:
                    documents = get_documents(portfolio_id, conn)
                    for i, document in enumerate(documents):
                        document_id = document[0]
                        #filename = "{}/{}_{}.pdf".format(carpeta, portfolio_id, i)
                        filename = "{}/{}.pdf".format(carpeta, document_id)
                        try:
                            if not exists(filename):
                                download_file(document_id, filename)
                                bar(1/len(documents))
                            else:
                                bar(1/len(documents), skipped=True)
                            downloaded += 1
                        except Exception as e:
                            print(f"  [ERROR] documento {document_id} de {portfolio_id}: {e}")
                except Exception as e:
                    print(f"[ERROR] portafolio {portfolio_id}: {e}")

                writer.writerow([portfolio_id, downloaded])
                outfile.flush()
                print(f"({idx}/{total}) {portfolio_id} -> {downloaded} documentos")

    conn.close()
    print(f"Listo. Resultados en {output_csv}")
