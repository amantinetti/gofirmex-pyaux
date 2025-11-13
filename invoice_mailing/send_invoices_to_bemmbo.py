from pathlib import Path
import os
import json
import requests

url_mails = "https://api.mailgun.net/v3/mg.gofirmex.cloud/messages"
headers_mails = {
    'Authorization': 'Basic YXBpOmY1NTM2MDBlN2U2OTQxYWQzODk1YzhmZTk5YjgwYWViLWZlOWNmMGE4LTFlODI0NGJm'
}


if __name__ == '__main__':
    ## First Send
    #begin = 495
    #end = 500

    ## Second Send
    begin = 555
    end = 600

    for i in range(begin, end):
        folder = Path("./facturas")
        try:
            pdf_file_path = list(folder.glob("*-FVAELECT-{}.pdf".format(i)))[0]
            xml_file_path = list(folder.glob("*_FVAELECT_{}.xml".format(i)))[0]
        except:
            print("No file found for invoice {}".format(i))
            continue

        variables = {
            "corporate_name": "bemmbo",
            "invoice_date": "00/00/00",
            "invoice_number": i,
            "invoice_total": "$0"
        }

        with open(pdf_file_path, 'rb') as pdf_file:
            with open(xml_file_path, 'rb') as xml_file:
                files = [
                    ("attachment", (os.path.basename(pdf_file_path), pdf_file)),
                    ("attachment", (os.path.basename(xml_file_path), xml_file)),
                ]

                payload = {'from': 'no-reply@mg.gofirmex.cloud',
                           'to': "dte@bemmbo.com",
                           'template': 'corporate-invoice-cl',
                           't:variables': json.dumps(variables)
                           }

                response = requests.request("POST", url_mails, headers=headers_mails, data=payload, files=files)
                print(response.text)

                xml_file.close()
                pdf_file.close()