import base64
import requests
import json
import psycopg2
import urllib.request
from alive_progress import alive_bar
from os.path import exists
import csv


def get_status(conn, status_name):
    sql = "select id, name from {}".format(status_name)
    cursor = conn.cursor()
    cursor.execute(sql, )
    status = cursor.fetchall()
    cursor.close()

    return status


def update_status(conn, status_name, name, id):
    sql = "update {} set name = %s where id = %s".format(status_name)
    cursor = conn.cursor()
    cursor.execute(sql, (name, id,))
    conn.commit()
    cursor.close()


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

    statuses_name = ["portfolio_statuses", "sign_document_statuses", "sign_document_signer_statuses"]
    conn = signing_conn

    for status_name in statuses_name:
        statuses = get_status(conn, status_name)
        print("Updating status for {}...".format(status_name))
        with alive_bar(len(statuses), force_tty=True) as bar:
            for status in statuses:
                name = status[1].upper().replace(" ", "_")
                update_status(conn, status_name, name, status[0])
                bar()

    documental_conn.close()
    signing_conn.close()