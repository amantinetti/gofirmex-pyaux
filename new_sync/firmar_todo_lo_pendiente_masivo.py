import requests
import json
import psycopg2

from concurrent.futures import ThreadPoolExecutor
from alive_progress import alive_bar


def get_documents(conn, rut):
    sql = """
          select sign_document_id, signer_id
          from sign_document_signers
          where signer_id in (select signers.id
                              from signers
                                       join (select portfolio_id,
                                                    max("order")                                          as max_order,
                                                    max(case when signed_at is not null then "order" end) as max_signed_order,
                                                    min(case when signed_at is null then "order" end)     as min_not_signed_order
                                             from signers
                                             group by portfolio_id
                                             order by max_order DESC) as d on d.portfolio_id = signers.portfolio_id
                                       join portfolios on signers.portfolio_id = portfolios.id
                                       join portfolio_statuses on portfolios.status_id = portfolio_statuses.id
                              where signers.nin = %s
                                and portfolio_statuses.name = 'IN_SIGNING'
                                and ((signers."order" = 0 and d.max_order = 0) or
                                     (signers."order" >= d.max_signed_order and
                                      signers."order" <= d.min_not_signed_order))
                                and signers.signs = false
                                and signers.signed_at is null)
            and signed_at is null;"""
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


def get_certificate(rut):
    # url = "http://localhost:3000/signer/sign-document"
    url = "http://10.142.0.16/ms/ex-sign-esign/v1/person/by-rut/{}".format(rut)
    headers = {
        'Content-Type': 'application/json'
    }

    response = requests.request("GET", url, headers=headers)

    if response.status_code == 201:
        esign = response.json()

        url = "http://10.142.0.16/ms/ex-sign-esign/v1/person/get-certificate"

        payload = json.dumps({
            "person_id": esign['data']['id'],
            "certificate_type": "FEA"
        })
        headers = {
            'Content-Type': 'application/json'
        }

        response = requests.request("POST", url, headers=headers, data=payload)
        return response.json()['data']['certificate_id']

    return None


if __name__ == '__main__':
    signing_conn = psycopg2.connect(database="signing_layer",
                                    host="10.2.0.2",
                                    user="signing_layer",
                                    password="kWn8W63Wxc6yJteoKEAhboNLiiMTMJ",
                                    port="5432", application_name="manti-python-script")

    ruts = ['13698503-5', '15735905-3', '12108216-0']

    for rut in ruts:
        print("Rut -> {}\n".format(rut))
        certificate = get_certificate(rut)
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
