import json
import os
import sys
import xml.etree.ElementTree as ET

import psycopg2
import requests
from alive_progress import alive_bar

from billing.models.factura import Factura

NS = {"sii": "http://www.sii.cl/SiiDte"}


def extract_section_txt(section, tag):
    el = section.find(f"sii:{tag}", NS)
    return el.text.strip() if el is not None and el.text else None


def extract_xml_fields(xml_path):
    """
    Devuelve una lista de dicts con los campos requeridos
    para cada Documento encontrado dentro del XML.
    """
    try:
        # Soporta XML con encoding ISO-8859-1; ET lo maneja automáticamente
        tree = ET.parse(xml_path)
        root = tree.getroot()
    except Exception as e:
        sys.stderr.write(f"[WARN] No se pudo parsear {xml_path}: {e}\n")
        return []

    # Buscar todos los nodos Documento (pueden venir varios DTE/Documento)
    documentos = root.findall(".//sii:DTE/sii:Documento", NS)
    resultados = []

    for doc in documentos:
        receptor = doc.find(".//sii:Encabezado/sii:Receptor", NS)
        if receptor is None:
            continue

        head = doc.find(".//sii:Encabezado/sii:IdDoc", NS)
        if head is None:
            continue

        totales = doc.find(".//sii:Encabezado/sii:Totales", NS)
        if totales is None:
            continue

        entry = {
            "tipo": extract_section_txt(head, "TipoDTE"),
            "folio": extract_section_txt(head, "Folio"),
            "emision": extract_section_txt(head, "FchEmis"),
            "vencimiento": extract_section_txt(head, "FchVenc"),
            "rut": extract_section_txt(receptor, "RUTRecep"),
            "razon_social": extract_section_txt(receptor, "RznSocRecep"),
            "giro": extract_section_txt(receptor, "GiroRecep"),
            "direccion": extract_section_txt(receptor, "DirRecep"),
            "comuna": extract_section_txt(receptor, "CmnaRecep"),
            "neto": extract_section_txt(totales, "MntNeto"),
            "iva": extract_section_txt(totales, "IVA"),
            "total": extract_section_txt(totales, "MntTotal"),
        }

        fact = Factura.from_raw(entry)
        return fact

        # Solo agregar si hay al menos el RUT o razón social
        if any(entry.values()):
            resultados.append(entry)


def gather_xml_files(path):
    if os.path.isdir(path):
        return [
            os.path.join(path, f)
            for f in os.listdir(path)
            if f.lower().endswith(".xml")
        ]
    elif os.path.isfile(path):
        return [path]
    else:
        raise FileNotFoundError(f"No existe la ruta: {path}")


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
             VALUES (%s, %s, 44, %s, %s, 'CLP', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id"""

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

    files = gather_xml_files("../invoice_mailing/facturas/")
    ##files = [f for f in files if "457" in f]
    files.sort(reverse=True)

    with alive_bar(len(files), force_tty=True) as bar:
        for f in files:
            fact = extract_xml_fields(f)
            city_id = None
            comuna = get_comuna(documental_conn, fact.comuna)
            if comuna is not None:
                city_id = comuna[0]
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
