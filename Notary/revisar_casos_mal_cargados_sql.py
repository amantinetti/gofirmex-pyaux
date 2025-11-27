import base64
import requests
import json
import psycopg2
import urllib.request
from alive_progress import alive_bar
from os.path import exists
import csv

from Notary.upload_from_ajs import analyze_pdf

files_manager_url = "https://files-manager.cloud-run.gofirmex.cloud"

exclude = ['87c18639-ecc5-4175-b941-3fe9fc2aa2da',
           '35a2adb3-a713-4ab6-8eeb-b3cd7bc9ed24',
           '75e96cef-e130-46fd-adb3-8adef4ca85c7',
           '08bbe2cb-55b4-4745-b234-f25b81a99beb',
           '41eea0ae-873a-455c-8e75-900e7885d532',
           '33c88f2f-951c-4621-9281-ecf7aafec247',
           '74c2494e-a9c7-4ff6-b82a-cc4849dd5f30',
           '50edfedd-579a-48d8-9c08-14f5e2f3e45a',
           '9e48218b-ae05-40ce-9425-984ec61c3275',
           '08cbf38a-4968-4409-a51e-f86e9ef8d0f6',
           '59cb40f0-da25-4405-b77e-b0adb3f7b9b1',
           '9af13ecc-e306-412b-b85a-c4e72e97f8e5',
           '13b83682-ec8e-439a-830a-d77532c6dcb9',
           '77ef72f4-92da-45d1-89e4-563e5796905a',
           '8d61a99e-6936-4699-a1e1-a376557d338f',
           '25d53dad-8145-4ca4-8c2e-44929d4637f0',
           '8a83caba-e071-411d-80a8-2819e1034060',
           '64c16be2-c28d-4690-9db9-caa386a6539c',
           '9d1b3aac-0cba-49de-a136-fe7c038b22de',
           '7e2ede81-e287-42cc-8a1c-45850f2fd1bd',
           '18fc8a37-170d-4739-9f1a-1ecc7ed990f1',
           '60a8bd1c-18a1-4ca6-b451-27b695127e32',
           '7e91a9c9-c4ef-4c38-893a-70b90c4a5c39',
           '50edfedd-579a-48d8-9c08-14f5e2f3e45a',
           '15b6159d-3856-46b0-94ed-2d07825d50fb',
           '19d9f286-d1f1-40a7-a715-3560c7d02e02',
           '9f4a5546-0d07-4d68-92fa-76155674c0f7',
           '9d636d74-db97-407f-ac9e-940fc58a6f0e',
           '29acc5c9-bc73-41b6-bdb9-26fb86224cc4',
           '8ae75c7e-fc5e-44ef-b6ba-48cfd8335e10',
           '8f3ff41a-a39a-465c-8919-115fcb329b66',
           '50e052f1-6165-4921-b775-a0ea94de7992',
           '8cc111e3-b8e2-49fd-9311-e8048b42facf',
           '42e3399d-8c4a-431f-8201-5e31afafcf88',
           '74c2494e-a9c7-4ff6-b82a-cc4849dd5f30',
           '65dc163f-936b-48be-b820-9ea3072a3d50',
           '51bc18a8-4571-419a-9c09-8c9dcb5dd3bd',
           '55b5d482-c19a-4ec5-8926-94d21196fd73',
           '8c6e21b4-1eff-4ed4-a7ae-7209ecaeb194',
           '46f1ea98-6369-4043-a802-8f229181956e',
           '9e53b115-3ccc-4934-9930-217cba8f4e91',
           '071a43ad-7667-4de2-a5c2-b34556796e4a',
           '46de5a83-96bd-42cf-af08-c2783969dda2',
           '9fdd6673-89ce-43d5-be7e-7deaa7d79ed7',
           '30aedc62-ce58-48f9-9bb6-dcbb2f20e5e4',
           '60e0362a-2ff7-4bab-be0a-22b9a18e1728',
           '9d440eba-cd85-41b0-b0f0-9012d667fecc',
           '10fdb953-5e7a-45c7-bba3-c26846f278a6',
           '8d9957cd-84f7-482a-8f55-398d9547a9a6',
           '7f5e018d-d2f7-4419-9e60-83b6fa67d74f',
           '3cadaee6-bf11-40ad-b0bc-0eb7cd36c365',
           'f554bebc-e075-4019-a2b4-655665ca4e70',
           '6e03c613-4df8-4ad3-bc1a-f1ad20368e15',
           '42c63200-43c7-43cc-8ff1-9560d0f3edad',
           '8a84c700-c833-46a1-abf0-927982433807',
           'c8314745-b8db-41c1-9d9b-edf637ffdc2a',
           'ed4cf5c5-5d8e-49af-beec-35c3a7d46e8a',
           '56fcbe46-9c06-4793-957c-d99384937331',
           '73ae08a9-2159-4199-a700-b013f8b8af8b',
           '21aeefbd-e6ae-4ac4-8919-dca02eb0960d',
           '33c88f2f-951c-4621-9281-ecf7aafec247',
           '41c9504e-4e23-437f-9fd0-b7abd55d6c87',
           '6178a2e3-35ca-4b01-950e-f3c203d5faac']


def get_file_url(document_id, doc_type="sign_document"):
    url = f'{files_manager_url}/documents/download_last_by_external_id'

    payload = json.dumps({
        "external_document_id": document_id,
        "external_document_type": doc_type,  ##"sign_document",
        "extended_expiration": True
    })
    headers = {
        'Content-Type': 'application/json'
    }

    response = requests.request("POST", url, headers=headers, data=payload)

    data = response.json()
    return data['document_url']


def download_file(document_id, doc_type="sign_document"):
    url = f'{files_manager_url}/documents/history_by_external_id'

    payload = json.dumps({
        "external_document_id": document_id,
        "external_document_type": doc_type,  ##"sign_document",
        "get_urls": True
    })
    headers = {
        'Content-Type': 'application/json'
    }

    response = requests.request("POST", url, headers=headers, data=payload)

    data = response.json()

    docsLen = len(data['documents'])
    filename = f'docs/FND-{document_id.upper()}.pdf'
    urllib.request.urlretrieve(data['documents'][docsLen - 2]['url'], filename)
    return filename


def get_signed_documents(conn, notary_id):
    sql = """select sign_documents.id
        from sign_documents
                 join portfolios on sign_documents.portfolio_id = portfolios.id
        where portfolios.notary_id = %s
          and portfolios.finished_at >= '2025-01-14 00:00:00'
          and sign_documents.notary_signing_type_id is not null
        order by portfolios.finished_at"""
    cursor = conn.cursor()
    cursor.execute(sql, (notary_id,))
    documents = cursor.fetchall()
    cursor.close()

    return documents


def process_document(document_id, file_write):
    file_url = get_file_url(document_id, 'sign_document')
    analysis_status = analyze_pdf(document_id, file_url)

    if not analysis_status:
        print("Caso a revisar ID {}".format(document_id))
        file_write.write(document_id + "\n")


if __name__ == '__main__':
    notary_conn = psycopg2.connect(database="notary_layer",
                                   host="10.2.0.2",
                                   user="notary_layer",
                                   password="YsB7cV9LWWDA4LenaGDsCCRz06fevi",
                                   port="5432", application_name="manti-python-script")

    documental_conn = psycopg2.connect(database="documental_layer",
                                       host="10.2.0.2",
                                       user="documental_layer",
                                       password="vKA5h2CZmf1p6EgQKBPRn8YpZDTmh2ay",
                                       port="5432", application_name="manti-python-script")

    file_read = open('redownload_docs', 'r')
    lines = file_read.readlines()
    for line in file_read:
        exclude.append(line.strip())
    file_read.close()

    file_write = open('redownload_docs', 'a')

    notary_id = '0eef8612-a840-4d06-9821-ce49278f8089'
    check_documents = get_signed_documents(documental_conn, notary_id)

    documents = []

    for i, sql_line in enumerate(check_documents):
        document_id = sql_line[0]

        if document_id in exclude:
            continue
        documents.append(document_id)

    with alive_bar(len(documents), force_tty=True) as bar:
        for i, document_id in enumerate(documents):
            process_document(document_id, file_write)
            bar()

    documental_conn.close()
    notary_conn.close()
    file_write.close()
