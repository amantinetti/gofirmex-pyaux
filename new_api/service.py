import os

import requests
import json
import base64
import new_api.encryption

import sys

main_url = "http://10.142.0.16/ms/signflow-api/v1"


#main_url = "http://localhost:3000"

def file_to_base64(filepath):
    with open(filepath, 'rb') as binary_file:
        binary_file_data = binary_file.read()
        base64_encoded_data = base64.b64encode(binary_file_data)
        base64_message = base64_encoded_data.decode('utf-8')

    return base64_message


def file_to_encrypted_base64(filepath, key):
    with open(filepath, 'rb') as binary_file:
        binary_file_data = binary_file.read()
        base64_message = new_api.encryption.encrypt(binary_file_data, key)

    return base64_message


def load_id_cards(signers, portfolio_id, customer_id):
    for signer in signers:
        # Load Front
        file_front = signer['front']
        document = {
            "description": "font id card",
            "document_type": "82bb382e-e55b-4f19-a61f-7ed0a4d521f9",
            "client_document_id": f"{signer['nin']}_front"
        }
        create_portfolio_signer_document(file_front, document, signer['nin'], portfolio_id, customer_id)

        # Load Front
        file_back = signer['back']
        document = {
            "description": "back id card",
            "document_type": "0720f3f0-6c9f-44d0-88e5-f4cd83cf9317",
            "client_document_id": f"{signer['nin']}_back"
        }
        create_portfolio_signer_document(file_back, document, signer['nin'], portfolio_id, customer_id)


def create_full_portfolio(documents, pfs_documents, signers, customer_id, start=True):
    portfolio_id = create_portfolio(signers, customer_id)
    print(f"Created Portfolio ID: {portfolio_id}")
    notarized = False

    for pfs_document in pfs_documents:
        doc = create_portfolio_signer_document(pfs_document['file_route'], pfs_document, pfs_document['nin'],
                                               portfolio_id, customer_id)
        print(f"\t Signer Document Created ID: {doc['data']['document']['id']}")

    for document in documents:
        if "notary" in document['signature'].lower() or "notario" in document['signature'].lower():
            notarized = True
        doc = create_document(document, portfolio_id, customer_id)
        print(f"\t Document Created ID: {doc['data']['document']['id']}")

    print(f"\t Documents Loaded")

    if notarized:
        load_id_cards(signers, portfolio_id, customer_id)
        print(f"\t Sign Documents Loaded")

    if start:
        start_portfolio_signing(portfolio_id, customer_id)
        print(f"\t Start Process")

    return portfolio_id


def create_portfolio(signers, customer_id, client_portfolio_id= "demo"):
    url = main_url + "/portfolio"

    payload = json.dumps({
        "signers": signers,
        "client_portfolio_id": client_portfolio_id
    })
    headers = {
        'Content-Type': 'application/json',
        'X-Consumer-Custom-ID': customer_id
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    return response.json()['data']['portfolio']['id']


def create_document(document, portfolio_id, customer_id, validate = True):
    url = main_url + "/portfolio_document"

    json_payload = {
        "description": document['description'],
        "document_type": document['document_type'],
        "portfolio_id": portfolio_id,
        "signature": document['signature'],
        "document": file_to_base64(document['file_route'])
    }

    if 'client_document_id' in document:
        json_payload['client_document_id'] = document['client_document_id']

    print("Tamaño del string: " + str(sys.getsizeof(json_payload['document'])))

    if 'encryption_key_string' in document:
        json_payload['document'] = file_to_encrypted_base64(document['file_route'], document['encryption_key_string'])
        json_payload['encryption_key'] = document['encryption_key']

        with open("encrypted.txt", 'w', encoding='utf-8') as file:
            file.write(json_payload['document'])

    payload = json.dumps(json_payload)
    headers = {
        'Content-Type': 'application/json',
        'X-Consumer-Custom-ID': customer_id
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    json_data = response.json()

    if validate:
        if not json_data['success']:
            print(f"  [ERROR] no se pudo crear el documento del portafolio {portfolio_id}")
            exit(1)

    return response.json()


def create_portfolio_signer_document(filepath, document, signer_nin, portfolio_id, customer_id, validate = True):
    url = main_url + "/portfolio_signer_document"

    b64_document = file_to_base64(filepath)

    payload = json.dumps({
        "description": document['description'],
        "document_type": document['document_type'],
        "client_document_id": document['client_document_id'],
        "signer_nin": signer_nin,
        "portfolio_id": portfolio_id,
        "document": b64_document
    })
    headers = {
        'Content-Type': 'application/json',
        'X-Consumer-Custom-ID': customer_id
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    json_data = response.json()

    if validate:
        if not json_data['success']:
            print(f"  [ERROR] no se pudo crear el documento del portafolio {portfolio_id}")
            exit(1)

    return response.json()


def start_portfolio_signing(portfolio_id, customer_id, validate = True):
    url = main_url + "/start_portfolio_signing"

    payload = json.dumps({
        "portfolio_id": portfolio_id
    })
    headers = {
        'Content-Type': 'application/json',
        'X-Consumer-Custom-ID': customer_id
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    json_data = response.json()

    if validate:
        if not json_data['success']:
            print(f"  [ERROR] no se pudo iniciar la firma del portafolio {portfolio_id}")
            exit(1)

    return response.json()


def get_correct_extension(filepath_without_extension):
    extensions = ['.png', '.jpg', '.jpeg']

    for ext in extensions:
        if os.path.isfile(filepath_without_extension + ext):
            return ext
    return None


def fix_image_extensions(signers):
    for signer in signers:
        if "front" in signer:
            base_path = signer['front'].rsplit('.', 1)[0]  # quita la extensión actual
            ext = get_correct_extension(base_path)
            if ext:
                signer['front'] = base_path + ext

        if "back" in signer:
            base_path = signer['back'].rsplit('.', 1)[0]  # quita la extensión actual
            ext = get_correct_extension(base_path)
            if ext:
                signer['back'] = base_path + ext

    return signers
