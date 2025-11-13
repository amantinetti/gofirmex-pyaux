import os
from pathlib import Path
import requests
from alive_progress import alive_bar


def upload_invoices(xml_file_path, pdf_file_path):
    url = "http://fincore.gofirmex.cloud/api/invoice-files/upload"
    #url = "http://localhost:8000/api/invoice-files/upload"

    payload = {}
    headers = {'Accept': 'application/json'}

    with open(pdf_file_path, 'rb') as pdf_file:
        with open(xml_file_path, 'rb') as xml_file:
            files = [
                ("files[]", (os.path.basename(pdf_file_path), pdf_file)),
                ("files[]", (os.path.basename(xml_file_path), xml_file)),
            ]

            response = requests.request("POST", url, headers=headers, data=payload, files=files)
            print(response.text)


if __name__ == '__main__':

    folder = Path("../invoice_mailing/facturas/")
    indices = range(309, 700)
    with alive_bar(len(indices), force_tty=True) as bar:
        for i in indices:
            try:
                pdf_file_path = list(folder.glob("*-FVAELECT-{}.pdf".format(i)))[0]
                xml_file_path = list(folder.glob("*_FVAELECT_{}.xml".format(i)))[0]
                upload_invoices(xml_file_path, pdf_file_path)
            except:
                print("No file found for invoice {}".format(i))
            bar()

