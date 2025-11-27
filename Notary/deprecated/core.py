import base64

import requests
import json
import psycopg2
import urllib.request

# files_manager_url = "http://10.142.0.16/ms/files-manager/v1"
files_manager_url = "https://files-manager.cloud-run.gofirmex.cloud"


def get_sign_document_data(document_id):
    conn = psycopg2.connect(database="documental_layer",
                            host="10.2.0.2",
                            user="documental_layer",
                            password="vKA5h2CZmf1p6EgQKBPRn8YpZDTmh2ay",
                            port="5432", application_name="manti-python-script")

    sql = "select document_types.name, portfolios.client_id, portfolios.id from sign_documents join document_types on sign_documents.type_id = document_types.id join portfolios on sign_documents.portfolio_id = portfolios.id where sign_documents.id = %s"

    cursor = conn.cursor()
    cursor.execute(sql, (document_id,))

    data = cursor.fetchone()

    conn.commit()
    cursor.close()

    sql = "select id from signers where portfolio_id = %s"
    cursor = conn.cursor()
    cursor.execute(sql, (data[2],))
    signers = cursor.fetchall()
    cursor.close()

    conn.close()

    return data[0], data[1], data[2], signers


def get_esign_signers_data(signer_id):
    conn = psycopg2.connect(database="ex_sign_esign",
                            host="10.2.0.2",
                            user="ex_sign_esign",
                            password="Xkt0WkVxKj23noXxRjnsnnsQN3JLad",
                            port="5432", application_name="manti-python-script")

    sql = "select people.rut, people.names, people.first_last_name, people.second_last_name, signer_documents.signed_at from signers join people on signers.person_id = people.id join signer_documents on signers.id = signer_documents.signer_id where signed_at is not null and signers.id = %s"

    cursor = conn.cursor()
    cursor.execute(sql, (signer_id,))

    data = cursor.fetchone()

    conn.commit()
    cursor.close()

    conn.close()

    if data is not None:
        name = "{} {}".format(data[1], data[2])
        if data[3] != "":
            name = "{} {}".format(name, data[3])
        return data[0], name, data[4], True

    print("Signs in Esign not found to {}".format(signer_id))
    return "", "", "", False


def get_signers_data(signer_id):
    conn = psycopg2.connect(database="sign_portal",
                            host="10.2.0.2",
                            user="sign_portal",
                            password="2WgiHxp03bCr9KPogT32QNH8ZaDbixpteBNH",
                            port="5432", application_name="manti-python-script")

    sql = "select internal_signers.cue_rut, internal_signers.cue_name, internal_signers.cue_father_lastname, internal_signers.cue_mother_lastname, signer_challenges.updated_at from internal_signers join signer_challenges on internal_signers.signer_id::uuid = signer_challenges.signer_id::uuid where signer_challenges.passed = true and internal_signers.signer_id =  %s"

    cursor = conn.cursor()
    cursor.execute(sql, (signer_id,))

    data = cursor.fetchone()

    conn.commit()
    cursor.close()

    if data is not None:
        return data[0], "{} {} {}".format(data[1], data[2], data[3]), data[4], True

    sql = "select internal_signers.cue_rut, internal_signers.cue_name, internal_signers.cue_father_lastname, internal_signers.cue_mother_lastname, signer_signs.updated_at from internal_signers join signer_signs on internal_signers.signer_id::uuid = signer_signs.signer_id::uuid where internal_signers.signer_id =  %s"

    cursor = conn.cursor()
    cursor.execute(sql, (signer_id,))
    data = cursor.fetchone()
    conn.commit()
    cursor.close()

    if data is not None:
        return data[0], "{} {} {}".format(data[1], data[2], data[3]), data[4], True

    sql = "select internal_signers.cue_rut, internal_signers.cue_name, internal_signers.cue_father_lastname, internal_signers.cue_mother_lastname, signer_signs.updated_at from internal_signers join signer_signs on internal_signers.cue_rut = signer_signs.rut where internal_signers.signer_id = %s"

    cursor = conn.cursor()
    cursor.execute(sql, (signer_id,))
    data = cursor.fetchone()
    conn.commit()
    cursor.close()

    if data is not None:
        return data[0], "{} {} {}".format(data[1], data[2], data[3]), data[4], True

    print("Signs in Portal not found to {}".format(signer_id))
    return "", "", "", False


def waiting_notary_document(document_id):
    conn = psycopg2.connect(database="documental_layer",
                            host="10.2.0.2",
                            user="documental_layer",
                            password="vKA5h2CZmf1p6EgQKBPRn8YpZDTmh2ay",
                            port="5432", application_name="manti-python-script")

    sql = "update sign_documents set status_id = 13 where id = %s"

    cursor = conn.cursor()
    cursor.execute(sql, (document_id,))

    conn.commit()
    cursor.close()

    conn.close()


def set_notarized_document(document_id):
    conn = psycopg2.connect(database="documental_layer",
                            host="10.2.0.2",
                            user="documental_layer",
                            password="vKA5h2CZmf1p6EgQKBPRn8YpZDTmh2ay",
                            port="5432", application_name="manti-python-script")

    sql = "update sign_documents set status_id = 14 where id = %s"

    cursor = conn.cursor()
    cursor.execute(sql, (document_id,))

    conn.commit()
    cursor.close()

    conn.close()


# def check_if_finalized(document_id):
#     conn = psycopg2.connect(database="documental_layer",
#                             host="10.2.0.2",
#                             user="documental_layer",
#                             password="vKA5h2CZmf1p6EgQKBPRn8YpZDTmh2ay",
#                             port="5432", application_name="manti-python-script")
#
#     sql = "select status_id from sign_documents where id = %s and status_id = 14"
#
#     cursor = conn.cursor()
#     cursor.execute(sql, (document_id,))
#
#     conn.commit()
#     cursor.close()
#
#     conn.close()


def upload_file_by_url(document_id, file_url, filename):
    url = f'{files_manager_url}/documents/upload'
    # url = "http://10.142.0.16/ms/files-manager/v1/documents/upload"

    payload = json.dumps({
        "external_document_id": document_id,
        "external_document_type": "sign_document",
        "file_url": file_url,
        "filename": filename
    })

    headers = {
        'Content-Type': 'application/json'
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    if response.status_code != 200 and response.status_code != 201:
        print("ERROR ON UPLOAD DOCUMENT")
        print(response.text)


def upload_file_b64(document_id, document, filename):
    url = f'{files_manager_url}/documents/upload'

    payload = json.dumps({
        "external_document_id": document_id,
        "external_document_type": "sign_document",
        "file": document,
        "filename": filename
    })

    headers = {
        'Content-Type': 'application/json'
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    if response.status_code != 200 and response.status_code != 201:
        print("ERROR ON UPLOAD DOCUMENT")
        print(response.text)
        return False
    return True


def not_signed_document(portfolio_id):
    conn = psycopg2.connect(database="documental_layer",
                            host="10.2.0.2",
                            user="documental_layer",
                            password="vKA5h2CZmf1p6EgQKBPRn8YpZDTmh2ay",
                            port="5432", application_name="manti-python-script")

    sql = "update sign_documents set status_id = 11 where portfolio_id = %s"

    cursor = conn.cursor()
    cursor.execute(sql, (portfolio_id,))

    conn.commit()
    cursor.close()

    sql = "update portfolios set status_id = 11 where id = %s"

    cursor = conn.cursor()
    cursor.execute(sql, (portfolio_id,))

    conn.commit()
    cursor.close()

    conn.close()


def get_file_url(document_id, type='sign_document'):
    url = f'{files_manager_url}/documents/download_last_by_external_id'

    payload = json.dumps({
        "external_document_id": document_id,
        "external_document_type": type,
        "extended_expiration": True
    })
    headers = {
        'Content-Type': 'application/json'
    }

    response = requests.request("POST", url, headers=headers, data=payload)

    data = response.json()
    return data['document_url']


def notary_printing(document_id, document_type, client_id, signers, file_url, certification_text, portfolio_id):
    signers_payload = []

    all_signers = True
    for signer in signers:
        nin, name, sign, status = get_signers_data(signer[0])
        if not status:
            nin, name, sign, status = get_esign_signers_data(signer[0])
        if status:
            signers_payload.append({
                "name": name,
                "nin": nin,
                "signAudit": signer,
                "signDate": sign.strftime("%Y/%m/%dT%H:%M:%S.000Z")
            })
        else:
            all_signers = False

    if not all_signers:
        # not_signed_document(portfolio_id)
        print("Not Signed Completed Portfolio {} Document {}".format(portfolio_id, document_id))
        return False

    url = "http://10.142.0.16/ms/notary-printing/v1/printing/process"

    payload = json.dumps({
        "clientId": client_id,
        "notaryText": certification_text,
        "pdfLegacy": False,
        "pdfSign": False,
        "signDocument": {
            "id": document_id,
            "idFromProvider": document_id,
            "providerName": "e-sign",
            "language": "spanish",
            "type": document_type,
            "url": file_url,
            "signingTypeName": "FEA",
        },
        "signers": signers_payload
    })

    headers = {
        'Content-Type': 'application/json'
    }

    proxies = {
        'http': 'http://10.142.0.60:8080',
        'https': 'http://10.142.0.60:8080',
    }

    # response = requests.request("POST", url, headers=headers, data=payload, proxies=proxies, verify=False)
    response = requests.request("POST", url, headers=headers, data=payload, proxies=proxies, verify=False)
    # print(response.text)
    return True


def download_file(document_id, type='sign_document'):
    url = f'{files_manager_url}/documents/download_last_by_external_id'

    payload = json.dumps({
        "external_document_id": document_id,
        "external_document_type": type,
        "extended_expiration": True
    })
    headers = {
        'Content-Type': 'application/json'
    }

    response = requests.request("POST", url, headers=headers, data=payload)

    data = response.json()
    filename = f'docs/FDI-{document_id.upper()}.pdf'
    urllib.request.urlretrieve(data['document_url'], filename)
    return filename


def file_to_base64(filepath):
    with open(filepath, 'rb') as binary_file:
        binary_file_data = binary_file.read()
        base64_encoded_data = base64.b64encode(binary_file_data)
        base64_message = base64_encoded_data.decode('utf-8')

    return base64_message


def process(document_id, notary_certification):
    # print("Proccessing Document ID -> {}".format(document_id))
    file_url = get_file_url(document_id)
    document_type, client_id, portfolio_id, signers = get_sign_document_data(document_id)
    # print("Document Type -> {} Client ID -> {} Portfolio ID -> {}".format(document_type, client_id, portfolio_id))

    status = notary_printing(document_id, document_type, client_id, signers, file_url, notary_certification,
                             portfolio_id)

    if status:
        # print("Set to Notary Pending")
        # waiting_notary_document(document_id)

        # print("Download File")
        download_file(document_id, 'notary_document')
    else:
        print("Upload First")
        # upload_file_by_url(document_id, file_url, "{}.pdf".format(document_id))
