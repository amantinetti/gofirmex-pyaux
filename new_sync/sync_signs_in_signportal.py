import base64
import requests
import json
import psycopg2
import urllib.request
from alive_progress import alive_bar
from os.path import exists
import csv


def update_documents_in_signing(conn, signer_id, portfolio_id, signed_at):
    sql = "select id from portfolios where id = %s"
    cursor = conn.cursor()
    cursor.execute(sql, (portfolio_id,))
    data = cursor.fetchone()
    cursor.close()

    if data is None:
        return None

    sql = "update portfolios set status_id = 12 where id = %s"
    cursor = conn.cursor()
    cursor.execute(sql, (portfolio_id,))
    conn.commit()
    cursor.close()

    sql = "update sign_documents set status_id = 8 where portfolio_id = %s"
    cursor = conn.cursor()
    cursor.execute(sql, (portfolio_id,))
    conn.commit()
    cursor.close()

    sql = "update sign_document_signers set status_id = 9, signed_at = %s where signer_id = %s"
    cursor = conn.cursor()
    cursor.execute(sql, (signed_at, signer_id,))
    conn.commit()
    cursor.close()

    sql = "update signers set signs = true, signed_at = %s where id = %s"
    cursor = conn.cursor()
    cursor.execute(sql, (signed_at, signer_id,))
    conn.commit()
    cursor.close()

    return data


def get_portfolio_status_in_demo(conn):
    sql = "select signer_id, portfolio_id, signed from signed_portfolios where signed is not null"
    cursor = conn.cursor()
    cursor.execute(sql, )
    data = cursor.fetchall()
    cursor.close()

    return data


if __name__ == '__main__':
    signing_portal_conn = psycopg2.connect(database="sign_portal",
                                           host="10.2.0.2",
                                           user="sign_portal",
                                           password="2WgiHxp03bCr9KPogT32QNH8ZaDbixpteBNH",
                                           port="5432", application_name="manti-python-script")

    documental_conn = psycopg2.connect(database="documental_layer",
                                       host="10.2.0.2",
                                       user="documental_layer",
                                       password="vKA5h2CZmf1p6EgQKBPRn8YpZDTmh2ay",
                                       port="5432", application_name="manti-python-script")

    signing_conn = psycopg2.connect(database="signing_layer",
                                    host="10.2.0.2",
                                    user="signing_layer",
                                    password="kWn8W63Wxc6yJteoKEAhboNLiiMTMJ",
                                    port="5432", application_name="manti-python-script")

    signed_portfolios = get_portfolio_status_in_demo(signing_portal_conn)

    with alive_bar(len(signed_portfolios), force_tty=True) as bar:
        for portfolio in signed_portfolios:
            signer_id = portfolio[0]
            portfolio_id = portfolio[1]
            signed_at = portfolio[2]
            data = update_documents_in_signing(signing_conn, signer_id, portfolio_id, signed_at)
            if data is not None:
                print("Update Data Signer ID -> {}".format(signer_id))
            bar()

    signing_portal_conn.close()
    documental_conn.close()
    signing_conn.close()
