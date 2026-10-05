import base64
import io
import sys
import xml.etree.ElementTree as ET

import requests
from pypdf import PdfReader

URL = 'https://ws.esigner.cl:8543/esign-orq/WSIntercambiaDocSoap'

RUTS = [
    "15312877-4",
    "21902869-5",
    "15935494-6",
    "15640278-8",
]
USER = 'gofirmex_assetplan_idyfdd'
PASSWORD = '7e43012d97c73e03ae09e76153e1da55da545624'
PIN = '1010'

ENVELOPE = '''<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:ws="http://ws.signserver.esign.com/">
   <soapenv:Header/>
   <soapenv:Body>
      <ws:intercambiaDocCoorPDFTSAPin>
         <Encabezado>
            <User>{user}</User>
            <Password>{password}</Password>
            <NombreConfiguracion>{configuration}</NombreConfiguracion>
            <PinFirma>{pin}</PinFirma>
         </Encabezado>
         <Parametro>
            <Documento>{document}</Documento>
            <NombreDocumento>{name}</NombreDocumento>
            <MetaData></MetaData>
            <UtilizaImagen>true</UtilizaImagen>
            <ImagenDinamica>true</ImagenDinamica>
            <PaginaImagen>0</PaginaImagen>
            <UtilizaTSA>false</UtilizaTSA>
            <NumeroPagina>{page}</NumeroPagina>
         </Parametro>
      </ws:intercambiaDocCoorPDFTSAPin>
   </soapenv:Body>
</soapenv:Envelope>'''


def last_page(b64):
    return len(PdfReader(io.BytesIO(base64.b64decode(b64))).pages)


def configuration(rut):
    return "774138315_PDF_{}_FEA".format(rut.replace("-", ""))


def sign(b64, rut, name='4d020c9a-50f2-46e5-bce8-6dea23c6ac98.pdf'):
    body = ENVELOPE.format(user=USER, password=PASSWORD, configuration=configuration(rut),
                           pin=PIN, document=b64, name=name, page=last_page(b64))
    response = requests.post(URL, data=body.encode('utf-8'),
                             headers={'Content-Type': 'application/xml'}, timeout=120)

    root = ET.fromstring(response.content)
    fault = root.find('.//{http://schemas.xmlsoap.org/soap/envelope/}Fault')
    if fault is not None:
        raise Exception(f"SOAP Fault: {fault.findtext('faultstring')}")
    response.raise_for_status()

    result = root.find('.//IntercambiaDocCoordPDFTSAPinResult')
    if result is None:
        raise Exception(f"Unexpected response: {response.text}")

    status = result.findtext('Estado')
    if status != 'OK':
        raise Exception(f"Sign failed ({status}): {result.findtext('Comentarios')}")

    return result.findtext('Documento')


if __name__ == '__main__':
    path = sys.argv[1] if len(sys.argv) > 1 else 'data/document.pdf'
    with open(path, 'rb') as f:
        b64 = base64.b64encode(f.read()).decode('ascii')

    # Each RUT signs the b64 returned by the previous signature
    for i, rut in enumerate(RUTS, start=1):
        b64 = sign(b64, rut)
        step_path = f'data/signed_doc_{i}_{rut}.pdf'
        with open(step_path, 'wb') as f:
            f.write(base64.b64decode(b64))
        print(f'{rut} signed -> {step_path}')

    with open('data/signed_doc.pdf', 'wb') as f:
        f.write(base64.b64decode(b64))
    print('data/signed_doc.pdf saved')
