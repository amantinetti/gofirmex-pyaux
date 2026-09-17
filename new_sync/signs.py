import base64
import requests
import json
import psycopg2
import urllib.request
from alive_progress import alive_bar
from os.path import exists
import csv


def get_finished_portfolios_in_signing(conn):
    sql = "select id from portfolios where status_id = 12"
    cursor = conn.cursor()
    cursor.execute(sql, )
    portfolios = cursor.fetchall()
    cursor.close()

    return portfolios


def get_signed_portfolios_in_documental(conn):
    sql = "select id from portfolios where status_id > 11"
    cursor = conn.cursor()
    cursor.execute(sql, )
    portfolios = cursor.fetchall()
    cursor.close()

    return portfolios


def get_portfolio_status_in_documental(conn, portfolio_id):
    sql = "select status_id from portfolios where id = %s"
    cursor = conn.cursor()
    cursor.execute(sql, (portfolio_id,))
    portfolio_status = cursor.fetchone()
    cursor.close()

    return portfolio_status[0]


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

    signed_portfolios = get_finished_portfolios_in_signing(signing_conn)
    documental_signed_portfolios = get_signed_portfolios_in_documental(documental_conn)

    with open('not_signed.csv', 'w', newline='') as csvfile:

        spamwriter = csv.writer(csvfile, delimiter=',',
                                quotechar='|', quoting=csv.QUOTE_MINIMAL)
        spamwriter.writerow(['portfolio_id'])
        csvfile.flush()

        with alive_bar(len(signed_portfolios), force_tty=True) as bar:
            for portfolio in signed_portfolios:
                portfolio_id = portfolio[0]
                if (portfolio_id,) not in documental_signed_portfolios:
                    status = get_portfolio_status_in_documental(documental_conn, portfolio_id)
                    if status == 11:
                        spamwriter.writerow([portfolio_id])
                        csvfile.flush()
                bar()

    documental_conn.close()
    signing_conn.close()
