from Notary.core import file_to_base64
import os
import re
import uuid
import requests
import json
from alive_progress import alive_bar


def replace_name(name):
    if "_1" in name or "_2" in name:
        print("ALERT DUPLICATED DOCUMENT")

    name = name.replace(".pdf", "")
    name = name.replace("039-FDI-", "")
    name = name.replace("039-FND-", "")
    name = name.replace("039-FNDP-", "")
    name = name.replace("FDI-", "")
    name = name.replace("FDI", "")
    name = name.replace("FND-", "")
    name = name.replace("FNDP-", "")
    name = name.replace("not_fejelomco_COPIA ", "")
    name = name.replace("not_adgonsalin_COPIA ", "")
    name = name.replace("COPIA T-", "")
    name = name.replace("COPIA D-", "")
    name = name.replace("COPIA C-", "")
    name = name.replace("COPIA B-", "")
    name = name.replace("COPIA A-", "")
    name = name.replace("COPIA F-", "")
    name = name.replace("COPIA ", "")
    name = name.replace("_OT_0", "")
    name = name.replace("Document_", "")
    name = name.replace(" (1)", "")
    name = name.replace("_1", "")
    name = name.replace(" ", "")
    name = re.sub("_.*", "", name)
    name = name.replace(".", "")
    doc_id = uuid.UUID(name)
    return str(doc_id)


def upload_to_notary(document_id, b64_file):
    url = "http://10.142.0.16/ms/notary-workflow-manager/v1/notary-portal/sign-document-sign"
    # url = "http://localhost:3000/notary-portal/sign-document-sign"

    payload = json.dumps({
        "signDocumentId": document_id,
        "file": b64_file,
        # "notaryID": "cca15b0d-2fd5-45a1-929e-229e8aa7024d"
        # "force": True,
        # "ignoreDocumentValidation": True,
    })
    headers = {
        'Content-Type': 'application/json'
    }

    response = requests.request("PUT", url, headers=headers, data=payload)
    if response.status_code != 200 and response.status_code != 201:
        if response.text == '{"statusCode":403,"message":"SignDocument was already signed by Notary and was not requested to be forced"}':
            return False
        print("ERROR ON UPLOAD DOCUMENT")
        print(response.text)
        raise Exception("Error on Upload Notary Document")
    return True


if __name__ == '__main__':
    documents = os.listdir("signed")
    if ".DS_Store" in documents:
        documents.remove(".DS_Store")

    total_size = len(documents)

    with alive_bar(total_size, force_tty=True) as bar:
        for i, document in enumerate(documents):
            try:
                document_id = replace_name(document)

                b64_file = file_to_base64(f'signed/{document}')

                status = upload_to_notary(document_id, b64_file)
                os.remove(f'signed/{document}')
                if status:
                    print(
                        "{}/{} ID {} Document {} uploaded status {}".format(i + 1, total_size, document_id,
                                                                            document,
                                                                            status))
                else:
                    print(
                        "{}/{} ID {} Document {} is already exists".format(i + 1, total_size, document_id,
                                                                           document))

            except Exception as e:
                print(
                    "{}/{} Document {} uploaded status FALSE ERROR --> {}".format(i + 1, total_size, document, e))
            bar()
