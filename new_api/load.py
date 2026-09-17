import new_api.service

signers = [{
    "nin": "19377149-1",
    "names": "Arturo",
    "lastnames": "Mantinetti",
    "email": "validate+a6wW7xW8ex@verify.unspam.email",
    "phone": "+56972824218",
    "notification": "email",
    "country": "CL",
    "front": "./documents/3107709_front.png",
    "back": "./documents/3107709_back.png"
}]

signers2 = [{
    "nin": "19377149-1",
    "names": "Arturo",
    "lastnames": "Mantinetti",
    "email": "amantinetti@gmail.com",
    "phone": "+56972824218",
    "notification": "email",
    "country": "CL",
    "front": "./documents/3107709_front.png",
    "back": "./documents/3107709_back.png",
    "order": 1,
}, {
    "nin": "18210843-K",
    "names": "Fabian",
    "lastnames": "Miranda",
    "email": "fabian@gofirmex.com",
    "phone": "+56999999999",
    "notification": "email",
    "country": "CL",
    "front": "./documents/3107709_front.png",
    "back": "./documents/3107709_back.png",
    "order": 2,
}]

signersFabi = [{
    "nin": "18210843-K",
    "names": "Fabian",
    "lastnames": "Miranda",
    "email": "fabian@gofirmex.com",
    "phone": "+56999999999",
    "notification": "email",
    "country": "CL",
    "front": "./documents/3107709_front.png",
    "back": "./documents/3107709_back.png",
}]

signersPipe = [{
    "nin": "20333259-9",
    "names": "Felipe",
    "lastnames": "Martellanz",
    "email": "felipe@gofirmex.com",
    "phone": "+56999999999",
    "notification": "email",
    "country": "CL",
    "front": "./documents/3107709_front.png",
    "back": "./documents/3107709_back.png",
}]

documents_ripley = [
    {
        "file_route": "./documents/document_ripley.pdf",
        "description": "contrato",
        "document_type": "e29a840f-703a-4f76-bacf-1d379ca9a9d5",
        "signature": "NOTARIO_ANCVB",
        "encryption_key": "b615ff30-94a0-404c-8fc3-1d7dab05c746",
        "encryption_key_string": "0EUtKrvGgvYiCFbKHmAiPiMCws7tde93",
    }
]

documents_ripley2 = [
    {
        "file_route": "./documents/contrato.pdf",
        "description": "contrato",
        "document_type": "e29a840f-703a-4f76-bacf-1d379ca9a9d5",
        "signature": "NOTARIO_ANCVB",
    }, {
        "file_route": "./documents/mandato.pdf",
        "description": "contrato",
        "document_type": "a1fad2fd-449d-4b45-afa4-23abd69e4a88",
        "signature": "FEA",
    }]

documents_multitest = [
    {
        "file_route": "./documents/contrato.pdf",
        "description": "contrato",
        "document_type": "e29a840f-703a-4f76-bacf-1d379ca9a9d5",
        "signature": "NOTARIO_ANCVB",
    }, {
        "file_route": "./documents/mandato.pdf",
        "description": "contrato",
        "document_type": "a1fad2fd-449d-4b45-afa4-23abd69e4a88",
        "signature": "FES",
    },
    {
        "file_route": "./documents/mandato.pdf",
        "description": "contrato",
        "document_type": "a1fad2fd-449d-4b45-afa4-23abd69e4a88",
        "signature": "FES + Notary Certification",
    }
]

documents_fes = [
    {
        "file_route": "./documents/mandato.pdf",
        "description": "contrato",
        "document_type": "a1fad2fd-449d-4b45-afa4-23abd69e4a88",
        "signature": "FES",
    },
    {
        "file_route": "./documents/mandato.pdf",
        "description": "contrato",
        "document_type": "a1fad2fd-449d-4b45-afa4-23abd69e4a88",
        "signature": "FES + Notary Certification",
    }
]

documents_migrantetest = [
    {
        "file_route": "./documents/mandato.pdf",
        "description": "contrato",
        "document_type": "a1fad2fd-449d-4b45-afa4-23abd69e4a88",
        "signature": "FEA",
    },
    {
        "file_route": "./documents/mandato.pdf",
        "description": "contrato",
        "document_type": "a1fad2fd-449d-4b45-afa4-23abd69e4a88",
        "signature": "FEA + Notary",
    }
]

documents_copec = [
    {
        "file_route": "./documents/contrato.pdf",
        "description": "contrato",
        "document_type": "e29a840f-703a-4f76-bacf-1d379ca9a9d5",
        "signature": "FEA + Notary Copec",
    }
]

documents5 = [
    {
        "file_route": "./documents/contrato.pdf",
        "description": "Mandato",
        "document_type": "a1fad2fd-449d-4b45-afa4-23abd69e4a88",
        "signature": "FES + Notary Certification",
    },
    {
        "file_route": "./documents/mandato.pdf",
        "description": "contrato",
        "document_type": "a1fad2fd-449d-4b45-afa4-23abd69e4a88",
        "signature": "FES + Notary Certification",
    }
]

documentsNotario2 = [
    {
        "file_route": "./documents/contrato.pdf",
        "description": "Mandato",
        "document_type": "a1fad2fd-449d-4b45-afa4-23abd69e4a88",
        "signature": "FES + Notary Certification",
    },
    {
        "file_route": "./documents/mandato.pdf",
        "description": "contrato",
        "document_type": "a1fad2fd-449d-4b45-afa4-23abd69e4a88",
        "signature": "FES + Notary Validated",
    }
]

documents4 = [
    {
        "file_route": "./documents/mandato.pdf",
        "description": "contrato",
        "document_type": "a1fad2fd-449d-4b45-afa4-23abd69e4a88",
        "signature": "FES",
    }
]

documents3 = [
    {
        "file_route": "./documents/mandato.pdf",
        "description": "contrato",
        "document_type": "a1fad2fd-449d-4b45-afa4-23abd69e4a88",
        "signature": "FEA + Notary",
    }
]

documents2 = [
    {
        "file_route": "./documents/contrato.pdf",
        "description": "Mandato",
        "document_type": "a1fad2fd-449d-4b45-afa4-23abd69e4a88",
        "signature": "FEA + Notary",
    },
    {
        "file_route": "./documents/mandato.pdf",
        "description": "contrato",
        "document_type": "a1fad2fd-449d-4b45-afa4-23abd69e4a88",
        "signature": "FEA",
    }
]

documents = [
    {
        "file_route": "./documents/contrato.pdf",
        "description": "contrato",
        "document_type": "a1fad2fd-449d-4b45-afa4-23abd69e4a88",
        "signature": "FES",
    }
]

#### Romo Propiedades
documents_romo = [
    {
        "file_route": "./romop/promesa.pdf",
        "description": "promesa",
        "document_type": "0295aadd-dc89-497f-a8d3-4693d8b4212b",
        "signature": "FEA + Notary",
    }
]

documents_romo2 = [
    {
        "file_route": "./romop/Res_prom.pdf",
        "description": "promesa",
        "document_type": "0295aadd-dc89-497f-a8d3-4693d8b4212b",
        "signature": "FEA + Notary",
    }
]

signers_romo = [{
    "nin": "14484621-4",
    "names": "SOLEDAD ALEJANDRA",
    "lastnames": "JOFRE BARRUECO",
    "email": "alejijofre@gmail.com",
    "notification": "email",
    "country": "CL",
    "front": "./romop/14484621_front.jpg",
    "back": "./romop/14484621_back.jpg"
}, {
    "nin": "11171290-5",
    "names": "GUILLERMO ARTURO",
    "lastnames": "LIRA REGUEIRA",
    "email": "gmolira@gmail.com",
    "notification": "email",
    "country": "CL",
    "front": "./romop/11171290_front.png",
    "back": "./romop/11171290_back.png"
}, {
    "nin": "9301404-9",
    "names": "MONICA DEL CORAZON DE JESUS",
    "lastnames": "LIRA REGUEIRA",
    "email": "escribe.mol@gmail.com",
    "notification": "email",
    "country": "CL",
    "front": "./romop/9301404_front.jpg",
    "back": "./romop/9301404_back.jpg"
}, {
    "nin": "12365867-1",
    "names": "ANITA",
    "lastnames": "LIRA REGUEIRA",
    "email": "anita.lira.r@gmail.com",
    "notification": "email",
    "country": "CL",
    "front": "./romop/12365867_front.png",
    "back": "./romop/12365867_back.png"
}, {
    "nin": "10209010-1",
    "names": "MARIA FERNANDA",
    "lastnames": "LIRA REGUEIRA",
    "email": "liraregueiramariafernanda@gmail.com",
    "notification": "email",
    "country": "CL",
    "front": "./romop/10209010_front.jpg",
    "back": "./romop/10209010_back.jpg"
}, {
    "nin": "7617518-7",
    "names": "JUAN ALBERTO",
    "lastnames": "ARANEDA NAVARRO",
    "email": "jaranedan@gmail.com",
    "notification": "email",
    "country": "CL",
    "front": "./romop/7617518_front.jpeg",
    "back": "./romop/7617518_back.jpeg"
}, {
    "nin": "9301403-0",
    "names": "MARIA FRANCISCA",
    "lastnames": "LIRA REGUEIRA",
    "email": "mariafranciscaliraregueira@yahoo.com",
    "notification": "email",
    "country": "CL",
    "front": "./romop/9301403_front.jpg",
    "back": "./romop/9301403_back.jpg"
}, {
    "nin": "8288780-6",
    "names": "MARCELA",
    "lastnames": "LIRA REGUEIRA",
    "email": "liraregueira.marcela@gmail.com",
    "notification": "email",
    "country": "CL",
    "front": "./romop/8288780_front.jpeg",
    "back": "./romop/8288780_back.jpeg"
}]

signers_romop2 = [{
    "nin": "8118908-0",
    "names": "MARIA PIA",
    "lastnames": "ZALDIVAR GRASS",
    "email": "mariapiazaldivar@gmail.com",
    "notification": "email",
    "country": "CL",
    "front": "./romop/8118908_front.jpeg",
    "back": "./romop/8118908_back.jpeg"
}, {
    "nin": "10698932-K",
    "names": "PAULA CAROLINA",
    "lastnames": "LEZAETA BICKELL",
    "email": "paula.lezaeta9@gmail.com",
    "notification": "email",
    "country": "CL",
    "front": "./romop/10698932_front.jpeg",
    "back": "./romop/10698932_back.jpeg"
}, {
    "nin": "4524943-3",
    "names": "FRANCISCO JAVIER",
    "lastnames": "ALEMPARTE COSTA",
    "email": "franciscoalemparte1940@gmail.com",
    "notification": "email",
    "country": "CL",
    "front": "./romop/4524943_front.jpeg",
    "back": "./romop/4524943_back.jpeg"
}, {
    "nin": "10934675-6",
    "names": "MARTA PAULINA",
    "lastnames": "ALEMPARTE BICKELL",
    "email": "paulina37@gmail.com",
    "notification": "email",
    "country": "CL",
    "front": "./romop/10934675_front.jpeg",
    "back": "./romop/10934675_back.jpeg"
}, {
    "nin": "10586103-6",
    "names": "FRANCISCO JAVIER",
    "lastnames": "ALEMPARTE BICKELL",
    "email": "fabmule@gmail.com",
    "notification": "email",
    "country": "CL",
    "front": "./romop/10586103_front.jpeg",
    "back": "./romop/10586103_back.jpeg"
}, {
    "nin": "14521981-7",
    "names": "HERNAN NICOLAS",
    "lastnames": "ALEMPARTE BICKELL",
    "email": "nicolas.alemparte76@gmail.com",
    "notification": "email",
    "country": "CL",
    "front": "./romop/14521981_front.jpeg",
    "back": "./romop/14521981_back.jpeg"
}, {
    "nin": "15367030-7",
    "names": "MARIA PIA",
    "lastnames": "ALEMPARTE BICKELL",
    "email": "piaalemparteb@gmail.com",
    "notification": "email",
    "country": "CL",
    "front": "./romop/15367030_front.jpeg",
    "back": "./romop/15367030_back.jpeg"
}]

documentsFES = [
    {
        "file_route": "./documents/contrato.pdf",
        "description": "Mandato",
        "document_type": "a1fad2fd-449d-4b45-afa4-23abd69e4a88",
        "signature": "FES",
    }
]

asset = "Contract_118250_155410"
documentoAssetplan = [
    {
        "file_route": "./assetplan/{}.pdf".format(asset),
        "description": asset,
        "document_type": "1f4830fb-f3df-4f30-a571-c77a12572a42",
        "signature": "FEA",
    }
]

signersAssetplan = [{
    "nin": "14119304-K",
    "names": "JOSÉ IGNACIO",
    "lastnames": "TORRETTI SCHMIDT",
    "email": "ignacio.torretti@assetplan.cl",
    # "phone": "+56972824218",
    "notification": "none",
    "country": "CL",
    # "front": "./documents/3107709_front.png",
    # "back": "./documents/3107709_back.png"
}]

documents_validated = [
    {
        "file_route": "./documents/contrato.pdf",
        "description": "documento demo",
        "document_type": "8eee82f0-9a63-4bd2-b3f3-9b681cff6129",
        "signature": "FES_NOTARY_VALIDATED",
    }
]

# customerID = "148ebf61-7383-400a-acae-cd824048434c" ## WaveDev
# customerID = "ceece6a8-698e-47fa-916f-b60c3ed71626"  ## Firmex
# customerID = "664f988f-a425-49a7-8611-d64b954de5a8"  ## Firmex Internal
# customerID = "0bf64ec7-9df5-44ba-b8c6-683e33e1010c"  ## Pruff
# customerID = "1ef952d0-0a0f-4b6c-b76f-1b75b7b90f3b"  ## Assetplan Asesores SpA
customerID = "7c9b9397-929f-4b17-b9db-09d5d27a247c"  ## Romo Propiedades SpA

if __name__ == '__main__':
    portfolio_id = new_api.service.create_full_portfolio(documents_romo2, [], signers_romop2, customerID, True)
