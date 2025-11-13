import json
from datetime import datetime, timedelta

import psycopg2
import requests
from alive_progress import alive_bar

from billing.models.factura import Factura


def read_json_ruts():
    try:
        with open('ruts.json', 'r') as file:
            data = json.load(file)
            return {item['RUTRecep'].upper(): item['organization'] for item in data}
    except FileNotFoundError:
        print("Error: ruts.json file not found")
        return {}
    except json.JSONDecodeError:
        print("Error: Invalid JSON format in ruts.json")
        return {}


def get_comuna(conn, name):
    sql = """select id, name
             from cities
             where upper(name) like %s"""
    cursor = conn.cursor()
    cursor.execute(sql, (f'%{name.upper()}%',))
    data = cursor.fetchone()
    cursor.close()
    return data


def get_document_type(conn, code):
    sql = """select id
             from document_code_types
             where code = %s"""
    cursor = conn.cursor()
    cursor.execute(sql, (str(code),))
    data = cursor.fetchone()
    cursor.close()
    return data


def get_invoice(conn, number):
    sql = """select id, status_id, paid_amount
             from invoices
             where invoice_number = %s"""
    cursor = conn.cursor()
    cursor.execute(sql, (number,))
    data = cursor.fetchone()
    cursor.close()
    return data


def create_invoice(conn, account_id, factura, city_id, document_code_type_id, status_id=2, paid_amount=0):
    sql = """INSERT INTO invoices (account_id, invoice_number, country_id, issue_date, due_date, currency,
                                   customer_tax_id, customer_name, customer_address, customer_city_id,
                                   document_code_type_id, subtotal_amount, tax_amount, total_amount, status_id,
                                   paid_amount)
             VALUES (%s, %s, 33, %s, %s, 'CLP', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id"""

    cursor = conn.cursor()
    cursor.execute(sql, (account_id, factura.folio, factura.emision, factura.vencimiento, factura.rut,
                         factura.razon_social, factura.direccion, city_id, document_code_type_id,
                         factura.neto, factura.iva, factura.total, status_id, paid_amount,))
    invoice_id = cursor.fetchone()[0]
    conn.commit()
    cursor.close()
    return invoice_id


def update_invoice_status(conn, invoice_id, status_id, paid_amount):
    sql = """UPDATE invoices
             SET status_id   = %s,
                 paid_amount = %s
             WHERE id = %s"""
    cursor = conn.cursor()
    cursor.execute(sql, (status_id, paid_amount, invoice_id,))
    conn.commit()
    cursor.close()


def get_billing_account(conn, organization_id):
    sql = """select id
             from accounts
             where organization_id = %s"""
    cursor = conn.cursor()
    cursor.execute(sql, (organization_id,))
    data = cursor.fetchone()
    cursor.close()
    return data


def get_bemmbo_invoice(number):
    url = "https://api.bemmbo.com/v1/invoices/issued?documentType=INVOICE&number={}".format(number)

    payload = {}
    headers = {
        'Authorization': 'eyJhbGciOiJIUzI1NiJ9.eyJvcmdhbml6YXRpb25JZCI6IjU3MTg5OWQ4LWQ2YmYtNDg2Ni1hMWZlLTBkY2IwZmQxZDI5NCIsImlhdCI6MTc0MjU2MTgxOSwiaXNzIjoidXJsOmh0dHBzOi8vYXBpLmJlbW1iby5jb20vdjEvYWRtaW4vZ2VuZXJhdGVXZWJUb2tlbiIsImF1ZCI6InVybDpodHRwczovL2FwaS5iZW1tYm8uY29tL3YxLyIsImV4cCI6MTc3NDExOTQxOX0.ngkCHOQqYbcYexR4v9CsEDLKg4WhN52Zo7L-D1A6c4E',
        'accept': 'application/json'
    }

    response = requests.request("GET", url, headers=headers, data=payload)
    data = response.json()

    if len(data['pageResults']) > 0:
        return data['pageResults'][0]


def simpleApiFacturas(month, year):
    url = "https://servicios.simpleapi.cl/api/RCV/ventas/{}/{}".format(month, year)

    payload = json.dumps({
        "RutUsuario": "77413831-5",
        "PasswordSII": "L3m0nSk7",
        "RutEmpresa": "77413831-5",
        "Ambiente": 1
    })
    headers = {
        'Content-Type': 'application/json',
        'Authorization': '4298-W020-6391-3376-0840'
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    ventas = response.json()['ventas']['detalleVentas']
    return ventas


def extact_factura(data):
    fecha = datetime.fromisoformat(data['fechaEmision'])  # ej. "2025-01-03T00:00:00"
    fecha_venc = fecha + timedelta(days=30)

    entry = {
        "tipo": data['tipoDte'],
        "folio": data['folio'],
        "emision": fecha.date().isoformat(),
        "vencimiento": fecha_venc.date().isoformat(),
        "rut": data['rutCliente'],
        "razon_social": data['razonSocial'],
        "giro": "-",
        "direccion": "-",
        "comuna": "",
        "neto": data['montoNeto'],
        "iva": data['montoIva'],
        "total": data['montoTotal'],
    }

    fact = Factura.from_raw(entry)
    return fact


if __name__ == '__main__':
    documental_conn = psycopg2.connect(database="documental_layer",
                                       host="10.2.0.2",
                                       user="documental_layer",
                                       password="vKA5h2CZmf1p6EgQKBPRn8YpZDTmh2ay",
                                       port="5432", application_name="manti-python-script")

    fincore_conn = psycopg2.connect(database="fincore",
                                    host="10.2.0.2",
                                    user="fincore",
                                    password="2Ne2QWkf2A9VKegk10ZetkcrUiKcui7mNmFYQwCE",
                                    port="5432", application_name="manti-python-script")

    ruts_map = read_json_ruts()
    print(f"Loaded {len(ruts_map)} RUTs from file")

    ventas = simpleApiFacturas(1, 2024)

    with alive_bar(len(ventas), force_tty=True) as bar:
        for venta in ventas:
            fact = extact_factura(venta)
            city_id = None

            if fact.tipo != 33:
                bar()
                continue

            dt = get_document_type(fincore_conn, fact.tipo)

            if fact.rut not in ruts_map:
                print("RUT {} FOUND Invoice Number {}".format(fact.rut, fact.folio))
                bar()
                continue

            billing_id = get_billing_account(fincore_conn, ruts_map[fact.rut])

            status = 2
            paid_amount = 0
            bemmbo_invoice = get_bemmbo_invoice(fact.folio)
            if bemmbo_invoice is not None:
                paid_amount = fact.total - bemmbo_invoice['availableAmount']
                if bemmbo_invoice['status'] == "PARTIALLY_PAID":
                    status = 4
                if bemmbo_invoice['status'] == "PAID" or bemmbo_invoice['status'] == "MANUALLY_MARKED_AS_PAID":
                    status = 5
                    paid_amount = fact.total
                if bemmbo_invoice['status'] == "NULLIFIED":
                    status = 6

            invoice = get_invoice(fincore_conn, fact.folio)

            if invoice is None:
                create_invoice(fincore_conn, billing_id, fact, city_id, dt[0], status, paid_amount)
            elif invoice[1] != status or invoice[2] != paid_amount:
                update_invoice_status(fincore_conn, invoice[0], status, paid_amount)
            bar()

    documental_conn.close()
    fincore_conn.close()
