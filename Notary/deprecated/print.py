from Notary.deprecated.core import process
from alive_progress import alive_bar
from os.path import exists

#  select *
#  from sign_documents
#           join portfolios on sign_documents.portfolio_id = portfolios.id
#  where sign_documents.notary_signing_type_id = 1
#    and (sign_documents.status_id = 11 or sign_documents.status_id = 12)
#    and (portfolios.status_id = 12 or portfolios.status_id = 11)
#    and portfolios.client_id = '6d78e6de-fd71-449b-9c70-1aaf888a49ee';

# notary_certification = ("RAFAEL ORLANDO CORVALÁN GUERRA, Abogado, Notario Reemplazante del Notario Público Titular de "
#                         "la Primera Notaría de La Reina, Pablo Martínez Loaiza, con oficio en Av. Príncipe de Gales "
#                         "5841, La Reina, CERTIFICO, conforme lo autoriza el artículo 401 número 6 del Código Orgánico "
#                         "de Tribunales, el siguiente hecho: Que el <documentType> que antecede, se obtuvo, suscrito "
#                         "con Firma Electrónica Avanzada en nombre de <signers>, "
#                         "en el sitio web "
#                         "https://www5.esigner.cl/esignercryptofront/documento/verificar. Santiago, <date>.")
# PROHIBICION DE GRAVAR Y ENAJENAR

notary_certification = ("ALVARO GONZALEZ SALINAS, abogado, Notario Público titular de SANTIAGO, con oficio en Av. Apoquindo 3001 Las Condes, AUTORIZO conforme lo autoriza el artículo 401 número 6 y 10 del Código Orgánico de Tribunales, el siguiente hecho: Que el <documentType> que antecede, se obtuvo, suscrito con Firma Electrónica Avanzada en nombre de <signers>, en el sitio web https://www5.esigner.cl/esignercryptofront/documento/verificar. Santiago, <date>.")

if __name__ == '__main__':
    jump_exists = True
    file = open('../documents', 'r')
    lines = file.readlines()

    with alive_bar(len(lines), force_tty=True) as bar:
        for i, line in enumerate(lines):
            document_id = line.strip().lower()
            if jump_exists:
                filename = f'docs/FDI-{document_id.upper()}.pdf'
                if exists(filename):
                    print("Jump already exist file {}".format(document_id))
                    bar()
                    continue
            process(document_id, notary_certification)
            bar()