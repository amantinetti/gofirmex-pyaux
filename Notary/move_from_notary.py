import base64
import requests
import json
import psycopg2
import urllib.request
from alive_progress import alive_bar
from os.path import exists
import csv


def update_notary_id(conn, portfolio_id, notary_id):
    sql = "update portfolios set notary_id = %s where portfolios.id = %s"
    cursor = conn.cursor()
    cursor.execute(sql, (notary_id, portfolio_id,))
    conn.commit()
    cursor.close()


def update_notary_id_to_null(conn, portfolio_id):
    sql = "update portfolios set notary_id = null where portfolios.id = %s"
    cursor = conn.cursor()
    cursor.execute(sql, (portfolio_id,))
    conn.commit()
    cursor.close()


def get_pending_portfolios(conn, notary_id):
    sql = """ select portfolios.id
        from portfolios
        where status_id = 7
          and notary_id = %s"""
    cursor = conn.cursor()
    cursor.execute(sql, (notary_id,))
    portfolios = cursor.fetchall()
    cursor.close()

    return portfolios


def retry_process_printing(portfolio_id):
    ##http://10.142.0.16/ms/workflow-utilities/v1/reset/notary-workflow/portfolio/03eff028-25f5-48dc-b42a-0210b40ca1ff
    url = "http://10.142.0.16/ms/workflow-utilities/v1/reset/notary-workflow/portfolio/" + portfolio_id  # Force
    response = requests.request("PUT", url)
    print(f"POST -> {url} Response Status -> {response.status_code} Text -> {response.text}")


if __name__ == '__main__':

    #source_notary = "80cb094c-f3f5-494a-81e3-677505da48f1"  # 42°NOTARIA DE SANTIAGO ALVARO DAVID GONZALEZ SALINAS
    #to_notary = "0eef8612-a840-4d06-9821-ce49278f8089"  # Notaria 1° NOTARIA DE INDEPENDENCIA

    source_notary = "0eef8612-a840-4d06-9821-ce49278f8089" # Notaria 1° NOTARIA DE INDEPENDENCIA
    to_notary = "0d265acb-e618-408b-aa58-801a2f4a0889" # 2° Notaría de San Miguel Fabián Díaz Contreras


    notary_conn = psycopg2.connect(database="notary_layer",
                                   host="10.2.0.2",
                                   user="notary_layer",
                                   password="YsB7cV9LWWDA4LenaGDsCCRz06fevi",
                                   port="5432", application_name="manti-python-script")

    documental_conn = psycopg2.connect(database="documental_layer",
                                       host="10.2.0.2",
                                       user="documental_layer",
                                       password="vKA5h2CZmf1p6EgQKBPRn8YpZDTmh2ay",
                                       port="5432", application_name="manti-python-script")

    portfolios = get_pending_portfolios(notary_conn, source_notary)

    with alive_bar(len(portfolios), force_tty=True) as bar:
        for portfolio in portfolios:
            portfolio_id = portfolio[0]
            update_notary_id(documental_conn, portfolio_id, to_notary)
            update_notary_id_to_null(notary_conn, portfolio_id)
            retry_process_printing(portfolio_id)
            bar()

    notary_conn.close()
    documental_conn.close()
