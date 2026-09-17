from alive_progress import alive_bar

import new_api.service
from pathlib import Path
import csv
import subprocess

def reimprimir_pdf(entrada, salida):
    subprocess.run([
        "gs",
        "-sDEVICE=pdfwrite",
        "-dNOPAUSE", "-dBATCH", "-dQUIET",
        "-dPrinted=true",           # trata el doc como "impreso"
        "-dPDFSETTINGS=/prepress",  # máxima calidad
        f"-sOutputFile={salida}",
        entrada,
    ], check=True)

if __name__ == '__main__':
    carpeta = "./massive_assetplan_downloads"
    pdfs = list(Path(carpeta).rglob("*.pdf"))

    new_folder = "./proccessed_assetplan_downloads"

    with alive_bar(len(pdfs), force_tty=True) as bar:
        for pdf in pdfs:
            filename = "{}/{}.pdf".format(new_folder, pdf.stem)
            reimprimir_pdf(pdf.resolve(), filename)
            bar()
