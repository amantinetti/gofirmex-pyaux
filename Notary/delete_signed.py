import os
import re
import uuid

import psycopg2
import requests
import json
from alive_progress import alive_bar


def replace_name(name):
    name = name.replace("FDI-", "").replace(".pdf", "")
    name = name.replace("FDI", "")
    name = name.replace("FND-", "")
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
    name = name.replace("039-", "")
    name = name.replace(" ", "")
    name = re.sub("_.*", "", name)
    name = name.replace(".", "")
    doc_id = uuid.UUID(name)
    return str(doc_id)


def get_pending_documents(conn):
    sql = """ select sign_documents.id
        from sign_documents
                 join portfolios on sign_documents.portfolio_id = portfolios.id
                 join document_types on sign_documents.type_id = document_types.id
        where status_id = 7
          and notary_signed_at is null """
    cursor = conn.cursor()
    cursor.execute(sql, )
    documents = cursor.fetchall()
    cursor.close()

    doc_array = []

    for doc in documents:
        doc_array.append(doc[0])

    return doc_array


if __name__ == '__main__':

    notary_conn = psycopg2.connect(database="notary_layer",
                                   host="10.2.0.2",
                                   user="notary_layer",
                                   password="YsB7cV9LWWDA4LenaGDsCCRz06fevi",
                                   port="5432", application_name="manti-python-script")

    pending_documents = get_pending_documents(notary_conn)

    documents = os.listdir("signed")
    if ".DS_Store" in documents:
        documents.remove(".DS_Store")

    total_size = len(documents)

    with alive_bar(total_size, force_tty=True) as bar:
        for i, document in enumerate(documents):
            try:
                document_id = replace_name(document)

                if document_id not in pending_documents:
                    os.remove(f'signed/{document}')
                else:
                    print("{}/{} ID {} Document {} is not signed".format(i + 1, total_size, document_id, document))

            except Exception as e:
                print(
                    "{}/{} Document {} ERROR --> {}".format(i + 1, total_size, document, e))
            bar()
