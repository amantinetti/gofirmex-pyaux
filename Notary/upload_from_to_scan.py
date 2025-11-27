import PyPDF2

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


def replace_fnd(name):
    name = name.replace(".pdf", "")
    name = name.replace("FND_", "")
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


def old_get_id_from_pdf(file_path):
    reader = PyPDF2.PdfReader(f'to_scan/{file_path}')

    notaryPage = reader.pages[0]
    notaryPageText = notaryPage.extract_text()

    patron = r'FDI-[A-F0-9-]+'
    resultado = re.findall(patron, notaryPageText)

    document_id = ""
    for i in resultado:
        try:
            document_id = replace_name(i)
        except:
            pass

    if document_id == "":
        return None

    num_pages = len(reader.pages)
    string = "{}.pdf".format(document_id)

    certificationPage = reader.pages[num_pages - 2]
    text = certificationPage.extract_text()
    res_search = re.search(string, text)

    if res_search:
        return document_id

    return None


def get_id_from_pdf(file_path):
    reader = PyPDF2.PdfReader(f'to_scan/{file_path}')

    num_pages = len(reader.pages)
    certificationPage = reader.pages[num_pages - 2]
    text = certificationPage.extract_text()

    patron = r"FND_[a-fA-F0-9-]+\.pdf"
    resultado = re.findall(patron, text)

    document_id = ""
    for res in resultado:
        try:
            document_id = replace_fnd(res)
        except:
            pass

    if document_id == "":
        return None
    return document_id


if __name__ == '__main__':
    documents = os.listdir("to_scan")
    if ".DS_Store" in documents:
        documents.remove(".DS_Store")

    total_size = len(documents)

    is_already_exists = 0
    is_uploaded = 0

    with alive_bar(total_size, force_tty=True) as bar:
        for i, document in enumerate(documents):
            try:
                document_id = get_id_from_pdf(document)

                if document_id is None:
                    bar()
                    continue

                b64_file = file_to_base64(f'to_scan/{document}')
                status = upload_to_notary(document_id, b64_file)
                os.remove(f'to_scan/{document}')
                if status:
                    is_uploaded = is_uploaded + 1
                    print(
                        "{}/{} ID {} Document {} uploaded status {}".format(i + 1, total_size, document_id,
                                                                            document,
                                                                            status))
                else:
                    is_already_exists = is_already_exists + 1
                    print(
                        "{}/{} ID {} Document {} is already exists".format(i + 1, total_size, document_id,
                                                                           document))

            except Exception as e:
                print(
                    "{}/{} Document {} uploaded status FALSE ERROR --> {}".format(i + 1, total_size, document, e))
            bar()

    print("IS UPLOADED -> {}".format(is_uploaded))
    print("IS ALREADY EXISTS -> {}".format(is_already_exists))
