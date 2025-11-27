from Notary.deprecated.core import file_to_base64, upload_file_b64
import os
import re
import uuid
from alive_progress import alive_bar


def replace_name(name):
    name = name.replace("FDI-", "").replace(".pdf", "")
    name = name.replace("FDI", "")
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
    name = name.replace(" ", "")
    name = re.sub("_.*", "", name)
    name = name.replace(".", "")
    doc_id = uuid.UUID(name)
    return str(doc_id)


if __name__ == '__main__':
    documents = os.listdir("../signed")
    if ".DS_Store" in documents:
        documents.remove(".DS_Store")

    total_size = len(documents)

    with alive_bar(total_size, force_tty=True) as bar:
        for i, document in enumerate(documents):
            try:
                document_id = replace_name(document)

                b64_file = file_to_base64(f'signed/{document}')
                status = upload_file_b64(document_id, b64_file, document)
                if status:
                    print("Status TRUE")
                    #os.remove(f'signed/{document}')
                    #set_notarized_document(document_id)
                print(
                    "{}/{} ID {} Document {} uploaded status {}".format(i + 1, total_size, document_id, document, status))

            except Exception as e:
                print(
                    "{}/{} Document {} uploaded status FALSE ERROR --> {}".format(i + 1, total_size, document, e))
            bar()
