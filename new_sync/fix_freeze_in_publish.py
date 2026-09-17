import base64
import requests
import json
import psycopg2
import urllib.request
from alive_progress import alive_bar
from os.path import exists
import csv
from datetime import datetime, timedelta, timezone


def get_portfolios_with_problems(conn):
    # Obtén la hora actual
    now = datetime.now(timezone.utc)

    # Resta 6 minutos
    time_minus_6_minutes = now - timedelta(minutes=6)

    sql = """select id from portfolios where status_id = 11 and updated_at <= %s"""
    cursor = conn.cursor()
    cursor.execute(sql, (time_minus_6_minutes,))
    data = cursor.fetchall()
    cursor.close()

    return data


def retry_publish(portfolio_id):
    url = "http://10.142.0.16/ms/signing-workflow-manager/v1/workflow-utilities/retry/publish/portfolio/" + portfolio_id  # Retry
    response = requests.request("PUT", url)
    print(f"POST -> {url} Response Status -> {response.status_code} Text -> {response.text}")


if __name__ == '__main__':

    retry = True

    documental_conn = psycopg2.connect(database="documental_layer",
                                       host="10.2.0.2",
                                       user="documental_layer",
                                       password="vKA5h2CZmf1p6EgQKBPRn8YpZDTmh2ay",
                                       port="5432", application_name="manti-python-script")


    portfolios = get_portfolios_with_problems(documental_conn)

    with alive_bar(len(portfolios), force_tty=True) as bar:
        for portfolio in portfolios:
            portfolio_id = portfolio[0]
            retry_publish(portfolio_id)
            bar()

    documental_conn.close()
