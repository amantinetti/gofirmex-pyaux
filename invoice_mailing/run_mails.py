import os
import requests
import json
import sqlite3
from datetime import datetime
from pathlib import Path



DB_PATH = "./data.db"

url_mails = "https://api.mailgun.net/v3/mg.gofirmex.cloud/messages"
headers_mails = {
    'Authorization': 'Basic YXBpOmY1NTM2MDBlN2U2OTQxYWQzODk1YzhmZTk5YjgwYWViLWZlOWNmMGE4LTFlODI0NGJm'
}


def connectSql(db_path=DB_PATH):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row  # para dict-like rows
    return conn


def defontana_login():
    url = "https://api.defontana.com/api/Auth/EmailLogin?email=juan%40gofirmex.com&password=L3m0nSk7!."
    payload = {}
    headers = {
        'Accept': 'text/plain',
        'Authorization': 'Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJuYW1laWQiOiJBRDEyM0ZULUhHREY1Ni1LSTIzS0wtS0pUUDk4NzYtSEdUMTIiLCJ1bmlxdWVfbmFtZSI6ImNsaWVudC5sZWdhY3lAZGVmb250YW5hLmNvbSIsImh0dHA6Ly9zY2hlbWFzLm1pY3Jvc29mdC5jb20vYWNjZXNzY29udHJvbHNlcnZpY2UvMjAxMC8wNy9jbGFpbXMvaWRlbnRpdHlwcm92aWRlciI6IkFTUC5ORVQgSWRlbnRpdHkiLCJBc3BOZXQuSWRlbnRpdHkuU2VjdXJpdHlTdGFtcCI6IkdIVEQyMzQtS0xISjc4NjgtRkc0OTIzLUhKRzA4RlQ1NiIsImNvbXBhbnkiOiIyMDI0MDQxMDE2NDg0NDMxOTAwMSIsImNsaWVudCI6IjIwMjQwNDEwMTY0ODQ0MzE5MDAxIiwib2xkc2VydmljZSI6InZpc2lvbmFyeSIsInVzZXIiOiJBZG1pbiIsInNlc3Npb24iOiIxNzU0OTQ4OTgxIiwic2VydmljZSI6InZpc2lvbmFyeSIsImNvdW50cnkiOiJDTCIsImNvbXBhbnlfbmFtZSI6IkZJUk1FWCBTUEEiLCJjb21wYW55X2NvdW50cnkiOiJjbCIsInVzZXJfbmFtZSI6Ikp1YW4gIiwiZXhwaXJhdGlvbl9kYXRlIjoxNzU3OTgwODAwLCJjbGllbnRfY29uZGl0aW9uIjoiUyIsInJvbGVzUG9zIjoiW1widXN1YXJpb1wiLFwidXN1YXJpb2VycFwiLFwicm9vdGVycFwiXSIsInJ1dF91c3VhcmlvIjoiQWRtaW5pc3RyYWRvciIsImlzcyI6Imh0dHBzOi8vKi5kZWZvbnRhbmEuY29tIiwiYXVkIjoiMDk5MTUzYzI2MjUxNDliYzhlY2IzZTg1ZTAzZjAwMjIiLCJleHAiOjE3ODY0ODY1OTEsIm5iZiI6MTc1NDk1MDU5MX0.Hi20_Z5Vea9f9ELvHXUB5h8IKZ-F0ZecY1lD91U1WA0'
    }
    response = requests.request("GET", url, headers=headers, data=payload)

    print(response.text)
    return response.json()['authResult']['access_token']


def defontana_ventas(token, page=0):
    url = "https://api-v2.defontana.com/api/sale?initialDate=2024-01-01T00:00:00.000Z&endingDate=2026-08-10T02:21:59.694Z&itemsPerPage=100&pageNumber={}&fromEntryDate=2024-01-01T00:00:00.000Z&toEntryDate=2026-08-10T02:21:59.694Z".format(
        page)

    payload = {}
    headers = {
        'Accept': 'text/plain',
        'Authorization': "Bearer {}".format(token)
    }

    response = requests.request("GET", url, headers=headers, data=payload)
    return response.json()


def update_bemmbo_already_paid():
    url = "https://api.bemmbo.com/v1/invoices/issued?options_pageSize=600&status=PAID,MANUALLY_MARKED_AS_PAID&documentType=INVOICE&emissiondateSince=2022-01-01T00:00:00Z"

    payload = {}
    headers = {
        'Authorization': 'eyJhbGciOiJIUzI1NiJ9.eyJvcmdhbml6YXRpb25JZCI6IjU3MTg5OWQ4LWQ2YmYtNDg2Ni1hMWZlLTBkY2IwZmQxZDI5NCIsImlhdCI6MTc0MjU2MTgxOSwiaXNzIjoidXJsOmh0dHBzOi8vYXBpLmJlbW1iby5jb20vdjEvYWRtaW4vZ2VuZXJhdGVXZWJUb2tlbiIsImF1ZCI6InVybDpodHRwczovL2FwaS5iZW1tYm8uY29tL3YxLyIsImV4cCI6MTc3NDExOTQxOX0.ngkCHOQqYbcYexR4v9CsEDLKg4WhN52Zo7L-D1A6c4E',
        'accept': 'application/json'
    }

    response = requests.request("GET", url, headers=headers, data=payload)

    return response.json()


def update_bemmbo_nulls():
    url = "https://api.bemmbo.com/v1/invoices/issued?options_pageSize=600&status=NULLIFIED&documentType=INVOICE&emissiondateSince=2022-01-01T00:00:00Z"

    payload = {}
    headers = {
        'Authorization': 'eyJhbGciOiJIUzI1NiJ9.eyJvcmdhbml6YXRpb25JZCI6IjU3MTg5OWQ4LWQ2YmYtNDg2Ni1hMWZlLTBkY2IwZmQxZDI5NCIsImlhdCI6MTc0MjU2MTgxOSwiaXNzIjoidXJsOmh0dHBzOi8vYXBpLmJlbW1iby5jb20vdjEvYWRtaW4vZ2VuZXJhdGVXZWJUb2tlbiIsImF1ZCI6InVybDpodHRwczovL2FwaS5iZW1tYm8uY29tL3YxLyIsImV4cCI6MTc3NDExOTQxOX0.ngkCHOQqYbcYexR4v9CsEDLKg4WhN52Zo7L-D1A6c4E',
        'accept': 'application/json'
    }

    response = requests.request("GET", url, headers=headers, data=payload)

    return response.json()


def insert_invoice_if_absent_no_unique(id, total, clientID, emiDate, expDate, conn):
    cur = conn.execute(
        """
        INSERT INTO invoices (id, total, client_id, emission_date, expiration_date)
        SELECT ?,
               ?,
               ?,
               ?,
               ? WHERE NOT EXISTS (SELECT 1 FROM invoices WHERE id = ?)
        """,
        (id, total, clientID, emiDate, expDate, id),
    )
    conn.commit()
    return cur.rowcount == 1  # True si insertó, False si ya existía


def update_invoice_if_exits(id, status_id, conn):
    cur = conn.cursor()
    cur.execute(
        "UPDATE invoices SET status = ? WHERE id = ?",
        (status_id, id),
    )

    conn.commit()
    return cur.rowcount > 0  # True si actualizo, False si ya existía


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

    ## Load Documents
    defontana_token = defontana_login()

    saleList = []
    lastPage = 1

    i = 0
    while i < lastPage:
        i += 1
        page = defontana_ventas(defontana_token, i)
        saleList.extend(page['saleList'])
        lastPage = page['totalItems'] // page['itemsPerPage'] + 1

    invoicesLog = {}

    for doc in saleList:
        if doc['documentType'] == "FVAELECT":
            id = doc['firstFolio']
            total = doc['total']
            clientID = doc['clientFile'].replace(".", "").upper()
            fecha_exp = datetime.fromisoformat(doc['expirationDate'])
            fecha_emi = datetime.fromisoformat(doc['emissionDate'])
            # fecha_exp_format = fecha_exp.strftime("%d/%m/%Y")
            # fecha_emi_format = fecha_emi.strftime("%d/%m/%Y")
            ### Document Status 0 Created 1 Email Sended 2 Expirated Email Sended 3 already payment
            result = insert_invoice_if_absent_no_unique(id, total, clientID, fecha_emi, fecha_exp, conn)

            invoicesLog[id] = {
                "total": total,
                "client_id": clientID,
                "expiration_date": fecha_exp,
                "emission_date": fecha_emi,
            }
            if result:
                print("Document id {} created".format(id, result))

    ## Update Already Paid Documents
    bemmbo_invoices = update_bemmbo_already_paid()['pageResults']
    for invoice in bemmbo_invoices:
        id = invoice['number']
        update_invoice_if_exits(id, 3, conn)

    ## Update Nulls
    bemmbo_invoices = update_bemmbo_nulls()['pageResults']
    for invoice in bemmbo_invoices:
        id = invoice['number']
        update_invoice_if_exits(id, -1, conn)

    ## Get invoices
    print("###################### UPDATE INVOICES ######################")
    invoices = get_pending_invoices(conn)
    for invoice in invoices:
        invoice_id = invoice['id']
        print("Invoice id -> {}".format(invoice_id))

        organization = get_organization(invoice['client_id'], conn)

        if organization is None:
            print("Organization id -> {} NOT FOUND".format(invoice['client_id']))
            continue

        folder = Path("./facturas")
        try:
            pdf_file_path = list(folder.glob("*-FVAELECT-{}.pdf".format(invoice_id)))[0]
            xml_file_path = list(folder.glob("*_FVAELECT_{}.xml".format(invoice_id)))[0]
        except:
            print("No file found for invoice {}".format(invoice_id))
            continue

        fecha_exp = datetime.fromisoformat(invoice['expiration_date'])
        fecha_emi = datetime.fromisoformat(invoice['emission_date'])
        fecha_exp_format = fecha_exp.strftime("%d/%m/%Y")
        fecha_emi_format = fecha_emi.strftime("%d/%m/%Y")

        variables = {
            "corporate_name": organization['name'],
            "invoice_date": fecha_emi_format,
            "invoice_number": invoice_id,
            "invoice_total": formato_peso_chileno(invoice['total'])
        }

        emails = "{},dte@bemmbo.com".format(organization['emails'].replace(" ", "")).split(",")

        subject = "GoFirmex Factura N {}".format(invoice_id)

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
                               'template': 'corporate-invoice-cl',
                               't:variables': json.dumps(variables)
                               }

                    response = requests.request("POST", url_mails, headers=headers_mails, data=payload, files=files)
                    print(response.text)

                    run = True

                    xml_file.close()
                    pdf_file.close()

        if sended == True and run == True:
            update_invoice_if_exits(invoice_id, 1, conn)

    conn.close()
    exit()
