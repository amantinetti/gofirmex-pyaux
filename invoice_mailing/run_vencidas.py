import os
import requests
import json
import sqlite3
from datetime import datetime
from pathlib import Path
import re
from dateutil import parser

DB_PATH = "./data.db"

url_mails = "https://api.mailgun.net/v3/mg.gofirmex.cloud/messages"
headers_mails = {
    'Authorization': 'Basic YXBpOmY1NTM2MDBlN2U2OTQxYWQzODk1YzhmZTk5YjgwYWViLWZlOWNmMGE4LTFlODI0NGJm'
}


def formatear_rut(rut: str) -> str:
    s = re.sub(r'[^0-9kK]', '', rut)  # quita puntos/guiones/espacios
    return f"{s[:-1]}-{s[-1].upper()}"


def connectSql(db_path=DB_PATH):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row  # para dict-like rows
    return conn


def bemmbo_overdue():
    url = "https://api.bemmbo.com/v1/invoices/issued?options_pageSize=600&status=OVERDUE&documentType=INVOICE&emissiondateSince=2022-01-01T00:00:00Z"

    payload = {}
    headers = {
        'Authorization': 'eyJhbGciOiJIUzI1NiJ9.eyJvcmdhbml6YXRpb25JZCI6IjU3MTg5OWQ4LWQ2YmYtNDg2Ni1hMWZlLTBkY2IwZmQxZDI5NCIsImlhdCI6MTc0MjU2MTgxOSwiaXNzIjoidXJsOmh0dHBzOi8vYXBpLmJlbW1iby5jb20vdjEvYWRtaW4vZ2VuZXJhdGVXZWJUb2tlbiIsImF1ZCI6InVybDpodHRwczovL2FwaS5iZW1tYm8uY29tL3YxLyIsImV4cCI6MTc3NDExOTQxOX0.ngkCHOQqYbcYexR4v9CsEDLKg4WhN52Zo7L-D1A6c4E',
        'accept': 'application/json'
    }

    response = requests.request("GET", url, headers=headers, data=payload)

    return response.json()


def get_pending_invoices(conn):
    query = ("SELECT * FROM invoices WHERE status = 0")
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    return [dict(row) for row in rows]


def get_organization(org_id, conn):
    cur = conn.cursor()
    cur.execute("SELECT * FROM organizations WHERE rut = ?", (org_id,))
    row = cur.fetchone()
    return dict(row) if row else None


def formato_peso_chileno(valor):
    return f"${valor:,.0f}".replace(",", ".")


if __name__ == '__main__':

    conn = connectSql()

    ## Get Expired Invoices
    bemmbo_invoices = bemmbo_overdue()['pageResults']
    for invoice in bemmbo_invoices:
        id = invoice['number']
        organization_rut = formatear_rut(invoice['customerFiscalId'])

        organization = get_organization(organization_rut, conn)

        if organization is None:
            print("Organization id -> {} NOT FOUND".format(organization_rut))
            continue

        fecha_exp = parser.isoparse(invoice['dueByDate'])
        fecha_emi = parser.isoparse(invoice['emissionDate'])
        fecha_exp_format = fecha_exp.strftime("%d/%m/%Y")
        fecha_emi_format = fecha_emi.strftime("%d/%m/%Y")

        variables = {
            "corporate_name": organization['name'],
            "invoice_date": fecha_emi_format,
            "invoice_exp_date": fecha_exp_format,
            "invoice_number": id,
            "invoice_total": formato_peso_chileno(invoice['totalAmount'])
        }

        folder = Path("./facturas")
        try:
            pdf_file_path = list(folder.glob("*-FVAELECT-{}.pdf".format(id)))[0]
            xml_file_path = list(folder.glob("*_FVAELECT_{}.xml".format(id)))[0]
        except:
            print("No file found for invoice {}".format(id))
            continue

        emails = "{}".format(organization['emails'].replace(" ", "")).split(",")

        subject = "GoFirmex Factura VENCIDA N {}".format(id)
        sended = True
        run = False
        for email in emails:
            with open(pdf_file_path, 'rb') as pdf_file:
                with open(xml_file_path, 'rb') as xml_file:
                    files = [
                        ("attachment", (os.path.basename(pdf_file_path), pdf_file)),
                        ("attachment", (os.path.basename(xml_file_path), xml_file)),
                    ]

                    payload = {'from': 'no-reply@mg.gofirmex.cloud',
                               'to': email,
                               'subject': subject,
                               'template': 'corporate-expired-invoice-cl',
                               't:variables': json.dumps(variables)
                               }

                    response = requests.request("POST", url_mails, headers=headers_mails, data=payload, files=files)
                    print(response.text)

                    run = True

                    xml_file.close()
                    pdf_file.close()
        if sended == True and run == True:
            print("GoFirmex Factura N {} Enviada".format(id))

    conn.close()
    exit()
