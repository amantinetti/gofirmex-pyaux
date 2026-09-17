import base64
import requests
import json
import psycopg2
import urllib.request
from alive_progress import alive_bar
from os.path import exists
import csv


def update_sovos_signer_name(conn, name, id):
    sql = "UPDATE \"document-signers\" SET name = %s where id = %s"
    cursor = conn.cursor()
    cursor.execute(sql, (name, id,))
    conn.commit()
    cursor.close()


def get_sovos_signer(conn, signer_id):
    sql = "select id, signer_id, name, document_id from \"document-signers\" where signer_id = %s"
    cursor = conn.cursor()
    cursor.execute(sql, (signer_id,))
    data = cursor.fetchone()
    cursor.close()

    return data


def get_signers_with_problems(conn):
    sql = """select signers.id, signers.names, signers.lastnames, signers.portfolio_id
        from signers
        join portfolios on signers.portfolio_id = portfolios.id
        where portfolios.client_id in ('75996f0d-7990-445b-847d-d500e8af436d', '16cd7db0-911e-4a7f-90d4-86b30ad7cb86')
        and portfolios.status_id = 5"""

    cursor = conn.cursor()
    cursor.execute(sql, )
    data = cursor.fetchall()
    cursor.close()

    return data


def retry_printing(document_id):
    url = "http://10.142.0.16/ms/notary-workflow-manager/v1/workflow-utilities/retry/sign-document/notary-printing/" + document_id  # Retry
    # url = "http://10.142.0.16/ms/notary-workflow-manager/v1/workflow-utilities/reprocess/retry/sign-document/notary-printing/" + document_id  # Force
    response = requests.request("PUT", url)
    print(f"POST -> {url} Response Status -> {response.status_code} Text -> {response.text}")


if __name__ == '__main__':

    retry = True

    sovos_conn = psycopg2.connect(database="ex_sign_sovos",
                                  host="10.2.0.2",
                                  user="ex_sign_sovos",
                                  password="szKX15q63yCF2eB6jthfABggwR7MwAbf",
                                  port="5432", application_name="manti-python-script")

    notary_conn = psycopg2.connect(database="notary_layer",
                                   host="10.2.0.2",
                                   user="notary_layer",
                                   password="YsB7cV9LWWDA4LenaGDsCCRz06fevi",
                                   port="5432", application_name="manti-python-script")

    signers = get_signers_with_problems(notary_conn)

    with alive_bar(len(signers), force_tty=True) as bar:
        for signer in signers:
            signer_id = signer[0]
            name = (signer[1] + " " + signer[2]).strip().replace('\n', '').replace('\t', '').replace('\r', '')

            sovos_signer = get_sovos_signer(sovos_conn, signer_id)

            try:
                if sovos_signer[2] == "" or sovos_signer[2] is None or sovos_signer[2].upper() != name.upper():
                    update_sovos_signer_name(sovos_conn, name, sovos_signer[0])
                    retry_printing(sovos_signer[3])
                    print("Update Data Signer ID -> {}".format(signer_id))
            except:
                print("No sovos_signer Found ID {} portfolio {}".format(signer_id, signer[3]))

            bar()

    notary_conn.close()
    sovos_conn.close()
