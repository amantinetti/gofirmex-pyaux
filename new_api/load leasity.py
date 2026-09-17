import new_api.service
from new_api.service import fix_image_extensions

signers2 = [
    {
        "nin": "4779552-4",
        "names": "MARÍA GLORIA",
        "lastnames": "CHICHARRO DÍAZ",
        "email": "mgchidi@gmail.com",
        "phone": "+56998853195",
        "notification": "email",
        "country": "CL",
        "front": "./leasity/4779552_front.jpg",
        "back": "./leasity/4779552_back.jpg"
    },
    {
        "nin": "10629942-0",
        "names": "NICOLÁS AUGUSTO",
        "lastnames": "FERNANDEZ CHICHARRO",
        "email": "soporte@leasity.cl",
        "phone": "+56998284938",
        "notification": "email",
        "country": "CL",
        "front": "./leasity/10629942_front.jpg",
        "back": "./leasity/10629942_back.jpg"
    },
    {
        "nin": "7080479-4",
        "names": "MARÍA LORETO",
        "lastnames": "MUÑOZ CHICHARRO",
        "email": "loretomch@yahoo.com",
        "phone": "+56992219081",
        "notification": "email",
        "country": "CL",
        "front": "./leasity/7080479_front.jpg",
        "back": "./leasity/7080479_back.jpg"
    },
    {
        "nin": "7080478-6",
        "names": "MARÍA SOLEDAD",
        "lastnames": "MUÑOZ CHICHARRO",
        "email": "soledadmchicharro@gmail.com",
        "phone": "+56993259561",
        "notification": "email",
        "country": "CL",
        "front": "./leasity/7080478_front.jpg",
        "back": "./leasity/7080478_back.jpg"
    },
    {
        "nin": "7014598-7",
        "names": "MAURICIO",
        "lastnames": "ROVIRA CHICHARRO",
        "email": "mauricio.rovira@hotmail.com",
        "phone": "+56998217517",
        "notification": "email",
        "country": "CL",
        "front": "./leasity/7014598_front.jpg",
        "back": "./leasity/7014598_back.jpg"
    },
    {
        "nin": "6598055-K",
        "names": "JOSÉ MIGUEL",
        "lastnames": "RESPALDIZA CHICHARRO",
        "email": "josemrespaldiza@gmail.com",
        "phone": "+56997011713",
        "notification": "email",
        "country": "CL",
        "front": "./leasity/6598055_front.jpg",
        "back": "./leasity/6598055_back.jpg"
    },
    {
        "nin": "6068348-4",
        "names": "MARÍA PATRICIA",
        "lastnames": "CHICHARRO BEIER",
        "email": "lapatch10@hotmail.com",
        "phone": "+56997798862",
        "notification": "email",
        "country": "CL",
        "front": "./leasity/6068348_front.jpg",
        "back": "./leasity/6068348_back.jpg"
    },
    {
        "nin": "24932580-5",
        "names": "JORGE ARMANDO",
        "lastnames": "URTECHO CALDERÓN",
        "email": "armando180291@gmail.com",
        "phone": "+56973588785",
        "notification": "email",
        "country": "CL",
        "front": "./leasity/24932580_front.jpg",
        "back": "./leasity/24932580_back.jpg"
    },
    {
        "nin": "16122632-7",
        "names": "RODRIGO OMAR",
        "lastnames": "CASTRO PALMA",
        "email": "rodrigocastr@gmail.com",
        "phone": "+56963401360",
        "notification": "email",
        "country": "CL",
        "front": "./leasity/16122632_front.jpg",
        "back": "./leasity/16122632_back.jpg"
    }
]

documents = [
    {
        "file_route": "./leasity/contrato_arriendo_1.pdf",
        "description": "contrato",
        "document_type": "1f4830fb-f3df-4f30-a571-c77a12572a42",
        "signature": "FEA + Notary",
    }
]

pfs_documents = [
    {
        "file_route": "./leasity/dominio_7080478.pdf",
        "description": "certificado titulo dominio",
        "document_type": "21401ec4-5e6e-4e4a-9b8a-02eac4abc3f7",
        "client_document_id": "doc",
        "nin": "7080478-6"
    },
    {
        "file_route": "./leasity/dominio_6068348.pdf",
        "description": "certificado titulo dominio",
        "document_type": "21401ec4-5e6e-4e4a-9b8a-02eac4abc3f7",
        "client_document_id": "doc",
        "nin": "6068348-4"
    },
    {
        "file_route": "./leasity/dominio_2.pdf",
        "description": "certificado titulo dominio",
        "document_type": "21401ec4-5e6e-4e4a-9b8a-02eac4abc3f7",
        "client_document_id": "doc",
        "nin": "16122632-7"
    },
    {
        "file_route": "./leasity/dominio_3.pdf",
        "description": "certificado titulo dominio",
        "document_type": "21401ec4-5e6e-4e4a-9b8a-02eac4abc3f7",
        "client_document_id": "doc",
        "nin": "16122632-7"
    },
    {
        "file_route": "./leasity/dominio_4.pdf",
        "description": "certificado titulo dominio",
        "document_type": "21401ec4-5e6e-4e4a-9b8a-02eac4abc3f7",
        "client_document_id": "doc",
        "nin": "16122632-7"
    },
]

customerID = "2e97dd80-2c86-47ff-8393-0cf5371b70c7"  ## Leasity
# customerID = "ceece6a8-698e-47fa-916f-b60c3ed71626"  ## Firmex

if __name__ == '__main__':
    # portfolio_id = new_api.service.create_full_portfolio(documents_ripley, signers, customerID, True)
    signers = new_api.service.fix_image_extensions(signers2)
    portfolio_id = new_api.service.create_full_portfolio(documents, pfs_documents, signers2, customerID, True)
