import base64
import requests
import json
import psycopg2
import urllib.request
from alive_progress import alive_bar
from os.path import exists
import csv
from datetime import datetime, timedelta


def get_portfolios_in_notary_in_documental(conn):
    sql = "select id, updated_at from portfolios where status_id = 12 order by updated_at ASC"
    cursor = conn.cursor()
    cursor.execute(sql, )
    portfolios = cursor.fetchall()
    cursor.close()

    return portfolios


def get_portfolio_status_in_notary(conn, portfolio_id):
    sql = "select status_id, notary_id from portfolios where id = %s"
    cursor = conn.cursor()
    cursor.execute(sql, (portfolio_id,))
    data = cursor.fetchone()
    cursor.close()

    return data


if __name__ == '__main__':

    retry = True

    documental_conn = psycopg2.connect(database="documental_layer",
                                       host="10.2.0.2",
                                       user="documental_layer",
                                       password="vKA5h2CZmf1p6EgQKBPRn8YpZDTmh2ay",
                                       port="5432", application_name="manti-python-script")

    notary_conn = psycopg2.connect(database="notary_layer",
                                   host="10.2.0.2",
                                   user="notary_layer",
                                   password="YsB7cV9LWWDA4LenaGDsCCRz06fevi",
                                   port="5432", application_name="manti-python-script")

    signed_portfolios = get_portfolios_in_notary_in_documental(documental_conn)

    with open('not_signed_notary.csv', 'w', newline='') as csvfile:

        spamwriter = csv.writer(csvfile, delimiter=',',
                                quotechar='|', quoting=csv.QUOTE_MINIMAL)
        spamwriter.writerow(['portfolio_id', 'problem', 'last_update'])
        csvfile.flush()

        with alive_bar(len(signed_portfolios), force_tty=True) as bar:
            for portfolio in signed_portfolios:
                portfolio_id = portfolio[0]
                last_update = portfolio[1]

                query = get_portfolio_status_in_notary(notary_conn, portfolio_id)
                if query is None:
                    spamwriter.writerow([portfolio_id, "no esta en bd", last_update])
                    bar()
                    continue

                status = query[0]
                notary_id = query[1]

                now = datetime.now()

                if status == 7 and notary_id is not None:
                    bar()
                    continue

                if status > 4 and notary_id is None:
                    spamwriter.writerow(
                        [portfolio_id, "sin signacion de notario y con status en notaria", last_update])
                    csvfile.flush()
                elif status == 8:
                    spamwriter.writerow(
                        [portfolio_id, "firmado en notaria pero no actualizado en documental", last_update])
                    csvfile.flush()
                elif status == 4:
                    spamwriter.writerow(
                        [portfolio_id, "pendiente de asignacion de notaria", last_update])
                    csvfile.flush()
                elif status == 5 and last_update < now - timedelta(minutes=10):
                    spamwriter.writerow([portfolio_id, "pegado en notary printing", last_update])
                    csvfile.flush()
                else:
                    spamwriter.writerow([portfolio_id, "portafolio con problemas", last_update])
                    csvfile.flush()
                bar()

    documental_conn.close()
    notary_conn.close()
