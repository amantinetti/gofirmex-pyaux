import base64
import requests
import json
import psycopg2
import urllib.request
from alive_progress import alive_bar
from os.path import exists
import csv


def update_document_finished_at(conn, finished_at, portfolio_id):
    sql = "UPDATE sign_documents SET finished_at = %s where portfolio_id = %s"
    cursor = conn.cursor()
    cursor.execute(sql, (finished_at, portfolio_id,))
    conn.commit()
    cursor.close()


def update_portfolio_finished_at(conn, finished_at, portfolio_id):
    sql = "UPDATE portfolios SET finished_at = %s where id = %s"
    cursor = conn.cursor()
    cursor.execute(sql, (finished_at, portfolio_id,))
    conn.commit()
    cursor.close()


def get_max_data(conn, portfolio_id):
    sql = "select max(updated_at) from sign_documents where portfolio_id = %s group by portfolio_id;"
    cursor = conn.cursor()
    cursor.execute(sql, (portfolio_id,))
    data = cursor.fetchone()
    cursor.close()

    return data


def get_portfolios_with_problem(conn):
    sql = """select portfolios.id
             from sign_documents
                      join portfolios on sign_documents.portfolio_id = portfolios.id
                      join clients on portfolios.client_id = clients.id
             where portfolios.finished_at >= '2026-01-21 00:00:00'
               and portfolios.finished_at < '2026-02-23 00:00:00'
               and portfolios.created_at <= '2025-12-01 00:00:00'
               and portfolios.finished_at > sign_documents.updated_at
               and sign_documents.status_id = 15
             --and portfolios.finished_at - portfolios.created_at > INTERVAL '3 months'
             --and clients.organization_id = 'ea1f1fee-c28b-48e0-991e-c18e14df1540'
             group by portfolios.id"""

    cursor = conn.cursor()
    cursor.execute(sql, )
    data = cursor.fetchall()
    cursor.close()

    return data


if __name__ == '__main__':

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

    portfolios = get_portfolios_with_problem(documental_conn)

    with alive_bar(len(portfolios), force_tty=True) as bar:
        for portfolio in portfolios:

            values = []
            result_signing = get_max_data(signing_conn, portfolio[0])
            if result_signing is not None:
                values.append(result_signing[0])
            result_documental = get_max_data(documental_conn, portfolio[0])
            if result_documental is not None:
                values.append(result_documental[0])

            if len(values) == 0:
                print("JUMPED ID: {}".format(portfolio[0]))
                bar()
                continue

            max_date = min(values)

            ##update_document_finished_at(documental_conn, max_date, portfolio[0])
            update_portfolio_finished_at(documental_conn, max_date, portfolio[0])
            print("Updated Portfolio ID: {}".format(portfolio[0]))

            bar()

    documental_conn.close()
    signing_conn.close()
