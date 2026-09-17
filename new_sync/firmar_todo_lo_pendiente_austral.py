import base64
import requests
import json
import psycopg2
import urllib.request
from alive_progress import alive_bar
from os.path import exists
import csv

from concurrent.futures import ThreadPoolExecutor
from threading import Lock
from alive_progress import alive_bar


def get_documents(conn, rut):
    sql = """
        select sign_document_id, signer_id
            from sign_document_signers
            where signer_id in (select signers.id
                                from signers
                                         join (select portfolios.id as portfolio_id
                                               from portfolios
                                                        join portfolio_statuses on portfolios.status_id = portfolio_statuses.id
                                               where portfolios.id in (select portfolio_id
                                                                       from (select portfolio_id,
                                                                                    count(*)         as signers,
                                                                                    count(signed_at) as signs_executed
                                                                             from signers
                                                                             group by portfolio_id) as d
                                                                       where d.signers = d.signs_executed + 1)
                                                 and portfolio_statuses.name = 'IN_SIGNING') as pfs
                                              on pfs.portfolio_id = signers.portfolio_id
                                where nin = %s
                                  and signers.signs = false
                                  and signers.signed_at is null)
                                  --and sign_document_id != '9c5ee32f-e50a-42c5-89ec-3fbc7eabb092'
              and signed_at is null"""
    cursor = conn.cursor()
    cursor.execute(sql, (rut,))
    data = cursor.fetchall()
    cursor.close()

    return data


def sign_document(signer_id, document_id, certificate_id):
    # url = "http://localhost:3000/signer/sign-document"
    url = "http://10.142.0.16/ms/ex-sign-esign/v1/signer/sign-document"

    payload = json.dumps({
        "singer_id": signer_id,
        "document_id": document_id,
        "origin": "manual",
        "send_kafka_status": True,
        "certificate_id": certificate_id,
        "credential_id": 2,
    })
    headers = {
        'Content-Type': 'application/json'
    }

    response = requests.request("POST", url, headers=headers, data=payload)

    if response.status_code == 201:
        print("Document ID -> {} Signed by -> {} Success".format(document_id, signer_id))
        return True
    print(response.text)


if __name__ == '__main__':
    signing_conn = psycopg2.connect(database="signing_layer",
                                    host="10.2.0.2",
                                    user="signing_layer",
                                    password="kWn8W63Wxc6yJteoKEAhboNLiiMTMJ",
                                    port="5432", application_name="manti-python-script")

    # rut = '12752188-3'  # RUT rector U austral old
    rut = '12108216-0'  # RUT rector U austral
    certificate = 'd75a7983-01eb-4fe6-ab60-9ffdb6174102'
    pending_documents = get_documents(signing_conn, rut)

    with alive_bar(len(pending_documents), force_tty=True) as bar:
        with ThreadPoolExecutor(max_workers=5) as executor:
            # Procesar documentos en hilos
            futures = [executor.submit(sign_document, document[1], document[0], certificate) for document in
                       pending_documents]
            for future in futures:
                future.result()  # Para manejar excepciones si ocurren
                bar()

    # with alive_bar(len(pending_documents), force_tty=True) as bar:
    #     for document in pending_documents:
    #         sign_document(document[1], document[0], certificate)
    #         bar()

    signing_conn.close()
