import base64
import requests
import json
import psycopg2
import urllib.request
from alive_progress import alive_bar
from os.path import exists
import csv

from time import sleep


def get_pending_portfolios(conn):
    sql = """ select id from portfolios where client_id = '75996f0d-7990-445b-847d-d500e8af436d' and status_id = 15 and created_at >= '2025-01-21 00:00:00' """
    cursor = conn.cursor()
    cursor.execute(sql, )
    documents = cursor.fetchall()
    cursor.close()

    return documents


def update_portfolio(portfolio_id):
    url = "http://10.142.0.16/ms/signing-workflow-manager/v1/workflow-utilities/retrieve/portfolio/signs/{}".format(
        portfolio_id)
    payload = {}
    headers = {}

    response = requests.request("PUT", url, headers=headers, data=payload)
    print("Portfolio ID -> {} Result -> {}".format(portfolio_id, response.text))


if __name__ == '__main__':

    documental_conn = psycopg2.connect(database="documental_layer",
                                       host="10.2.0.2",
                                       user="documental_layer",
                                       password="vKA5h2CZmf1p6EgQKBPRn8YpZDTmh2ay",
                                       port="5432", application_name="manti-python-script")

    for i in range(1001):

        portfolios = get_pending_portfolios(documental_conn)

        with alive_bar(len(portfolios), force_tty=True) as bar:
            for portfolio in portfolios:
                update_portfolio(portfolio[0])
                bar()

        sleep(150)

    documental_conn.close()
