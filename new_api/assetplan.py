import new_api.service
from pathlib import Path
import csv


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

customerID = "1ef952d0-0a0f-4b6c-b76f-1b75b7b90f3b"  ## Assetplan Asesores SpA


if __name__ == '__main__':
    carpeta = "./massive_assetplan"
    pdfs = list(Path(carpeta).rglob("*.pdf"))

    with open("assetplan.csv", "w", newline="", encoding="utf-8") as logfile:
        writer = csv.writer(logfile)
        writer.writerow(["portfolio_id", "new_portfolio_id"])
        for pdf in pdfs:
            exPortfolioID = pdf.stem
            documents = [
                {
                    "file_route": pdf.resolve(),
                    "description": "Nueva firma portafolio {}".format(exPortfolioID),
                    "document_type": "1f4830fb-f3df-4f30-a571-c77a12572a42",
                    "signature": "FEA",
                }
            ]
            portfolio_id = new_api.service.create_full_portfolio(documents, [], signersAssetplan, customerID, True)
            writer.writerow([exPortfolioID, portfolio_id])