import new_api.service
from new_api.service import fix_image_extensions

signers = [
    {
        "nin": "17011844-8",
        "names": "SEBASTIAN IGNACIO",
        "lastnames": "ORTIZ LANCELLOTTI",
        "email": "sortiz@lucanorent.cl",
        "phone": "+56995917371",
        "notification": "email",
        "country": "CL",
        "front": "./vs/17011844_front.jpeg",
        "back": "./vs/17011844_back.jpeg"
    }, {
        "nin": "5299445-4",
        "names": "FRANCISCO DE ASIS",
        "lastnames": "PAREDES DIAZ",
        "email": "natales@vspropiedades.cl",
        "phone": "+56997624884",
        "notification": "email",
        "country": "CL",
        "front": "./vs/5299445_front.jpeg",
        "back": "./vs/5299445_back.jpeg"
    },
    {
        "nin": "7751058-3",
        "names": "JORGE EPIFANIO",
        "lastnames": "PAREDES DIAZ",
        "email": "natales@vspropiedades.cl",
        "phone": "+56975400692",
        "notification": "email",
        "country": "CL",
        "front": "./vs/7751058_front.jpeg",
        "back": "./vs/7751058_back.jpeg"
    },
    {
        "nin": "4388358-5",
        "names": "DELIA",
        "lastnames": "SEGOVIA SEPULVEDA",
        "email": "natales@vspropiedades.cl",
        "phone": "+56968550133",
        "notification": "email",
        "country": "CL",
        "front": "./vs/4388358_front.jpeg",
        "back": "./vs/4388358_back.jpeg"
    },
    {
        "nin": "7496065-0",
        "names": "ANTONIO ANGEL",
        "lastnames": "ÁLVAREZ VALENZUELA",
        "email": "antonio070818@gmail.com",
        "phone": "+56991865883",
        "notification": "email",
        "country": "CL",
        "front": "./vs/7496065_front.jpeg",
        "back": "./vs/7496065_back.jpeg"
    },
    {
        "nin": "8266090-9",
        "names": "ELSA MARISOL",
        "lastnames": "PAREDES SEGOVIA",
        "email": "marisolparedes7464@gmail.com",
        "phone": "+56953185122",
        "notification": "email",
        "country": "CL",
        "front": "./vs/8266090_front.jpeg",
        "back": "./vs/8266090_back.jpeg"
    },
    {
        "nin": "8266088-7",
        "names": "DELIA JACQUELINE",
        "lastnames": "PAREDES SEGOVIA",
        "email": "delia67p@hotmail.com",
        "phone": "+56999647235",
        "notification": "email",
        "country": "CL",
        "front": "./vs/8266088_front.jpeg",
        "back": "./vs/8266088_back.jpeg"
    },

]

documents = [
    {
        "file_route": "./vs/contrato_vs.pdf",
        "description": "Promesa Compra Venta",
        "document_type": "0295aadd-dc89-497f-a8d3-4693d8b4212b",
        "signature": "FEA + Notary",
    }
]

pfs_documents = [
    {
        "file_route": "./leasity/titulo_dominio_4779552.pdf",
        "description": "certificado titulo dominio",
        "document_type": "21401ec4-5e6e-4e4a-9b8a-02eac4abc3f7",
        "client_document_id": "doc",
        "nin": "4779552-4"
    },
    {
        "file_route": "./leasity/titulo_dominio_6068348.pdf",
        "description": "certificado titulo dominio",
        "document_type": "21401ec4-5e6e-4e4a-9b8a-02eac4abc3f7",
        "client_document_id": "doc",
        "nin": "6068348-4",
    },
    {
        "file_route": "./leasity/titulo_dominio_7080478.pdf",
        "description": "certificado titulo dominio",
        "document_type": "21401ec4-5e6e-4e4a-9b8a-02eac4abc3f7",
        "client_document_id": "doc",
        "nin": "7080478-6",
    },
    {
        "file_route": "./leasity/titulo_dominio_7080479.pdf",
        "description": "certificado titulo dominio",
        "document_type": "21401ec4-5e6e-4e4a-9b8a-02eac4abc3f7",
        "client_document_id": "doc",
        "nin": "7080479-4",
    }
]

pfs = [
    {
        "file_route": "./leasity/titulo_dominio_4779552.pdf",
        "description": "certificado titulo dominio",
        "document_type": "21401ec4-5e6e-4e4a-9b8a-02eac4abc3f7",
        "client_document_id": "doc",
        "nin": "19377149-1"
    }
]

customerID = "dbf20029-f32f-46be-8fe1-9c2b96264c01"  ## Firmex

if __name__ == '__main__':
    # portfolio_id = new_api.service.create_full_portfolio(documents_ripley, signers, customerID, True)
    signers_fix = new_api.service.fix_image_extensions(signers)
    portfolio_id = new_api.service.create_full_portfolio(documents, pfs, signers_fix, customerID, False)
