import json
import os
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

import psycopg2
import requests
from alive_progress import alive_bar

from billing.models.factura import Factura

NS = {"sii": "http://www.sii.cl/SiiDte"}


def extract_section_txt(section, tag):
    el = section.find(f"sii:{tag}", NS)
    return el.text.strip() if el is not None and el.text else None


def extract_xml_fields(xml_path):
    """
    Devuelve una lista de dicts con los campos requeridos
    para cada Documento encontrado dentro del XML.
    """
    try:
        # Soporta XML con encoding ISO-8859-1; ET lo maneja automáticamente
        tree = ET.parse(xml_path)
        root = tree.getroot()
    except Exception as e:
        sys.stderr.write(f"[WARN] No se pudo parsear {xml_path}: {e}\n")
        return []

    # Buscar todos los nodos Documento (pueden venir varios DTE/Documento)
    documentos = root.findall(".//sii:DTE/sii:Documento", NS)
    resultados = []

    for doc in documentos:
        receptor = doc.find(".//sii:Encabezado/sii:Receptor", NS)
        if receptor is None:
            continue

        head = doc.find(".//sii:Encabezado/sii:IdDoc", NS)
        if head is None:
            continue

        totales = doc.find(".//sii:Encabezado/sii:Totales", NS)
        if totales is None:
            continue

        entry = {
            "tipo": extract_section_txt(head, "TipoDTE"),
            "folio": extract_section_txt(head, "Folio"),
            "emision": extract_section_txt(head, "FchEmis"),
            "vencimiento": extract_section_txt(head, "FchVenc"),
            "rut": extract_section_txt(receptor, "RUTRecep"),
            "razon_social": extract_section_txt(receptor, "RznSocRecep"),
            "giro": extract_section_txt(receptor, "GiroRecep"),
            "direccion": extract_section_txt(receptor, "DirRecep"),
            "comuna": extract_section_txt(receptor, "CmnaRecep"),
            "neto": extract_section_txt(totales, "MntNeto"),
            "iva": extract_section_txt(totales, "IVA"),
            "total": extract_section_txt(totales, "MntTotal"),
        }

        fact = Factura.from_raw(entry)
        return fact

        # Solo agregar si hay al menos el RUT o razón social
        if any(entry.values()):
            resultados.append(entry)


def gather_xml_files(path):
    if os.path.isdir(path):
        return [
            os.path.join(path, f)
            for f in os.listdir(path)
            if f.lower().endswith(".xml")
        ]
    elif os.path.isfile(path):
        return [path]
    else:
        raise FileNotFoundError(f"No existe la ruta: {path}")


def update_invoice_mode(conn, invoice_id, mode_id):
    sql = """UPDATE invoices
             SET invoicing_mode_id = %s
             WHERE invoice_number = %s"""
    cursor = conn.cursor()
    cursor.execute(sql, (mode_id, invoice_id,))
    conn.commit()
    cursor.close()


def get_pending_invoices(conn):
    sql = """select id, invoice_number, lower(customer_name), paid_amount, customer_tax_id
             from invoices
             where invoicing_mode_id is null
          """
    cursor = conn.cursor()
    cursor.execute(sql, )
    data = cursor.fetchall()
    cursor.close()
    return data


def has_strings(file_path: str, search_texts: list[str]) -> bool:
    try:
        # Intentamos leer en UTF-8, si falla usamos latin-1 como respaldo
        try:
            text = Path(file_path).read_text(encoding='utf-8')
        except UnicodeDecodeError:
            text = Path(file_path).read_text(encoding='latin-1')

        # Pasamos todo a minúsculas
        text = text.lower()

        # Buscamos los textos
        for search_text in search_texts:
            if search_text.lower() in text:
                return True

    except Exception as e:
        print(f"Error al procesar {file_path}: {e}")
        return False
    return False


def fix_string(s):
    return (s.lower()
            .replace("á", "a")
            .replace("é", "e")
            .replace("í", "i")
            .replace("ó", "o")
            .replace("ú", "u")
            .replace(".", "")
            .replace(" ", ""))


if __name__ == '__main__':
    fincore_conn = psycopg2.connect(database="fincore",
                                    host="10.2.0.2",
                                    user="fincore",
                                    password="2Ne2QWkf2A9VKegk10ZetkcrUiKcui7mNmFYQwCE",
                                    port="5432", application_name="manti-python-script")

    files = gather_xml_files("../invoice_mailing/facturas/")
    ##files = [f for f in files if "457" in f]
    files.sort(reverse=True)

    fact_data = {}
    for f in files:
        fact = extract_xml_fields(f)
        fact_data[fact.folio] = fact

    invoices = get_pending_invoices(fincore_conn)

    packs_social_names = [
        "INMOBILIARIA BD",
        "LEVANNTA SPA",
        "VOD GESTION INMOBILIARIA LIMITADA",
        "AGENCIA INMOBILIARIA Y CORRETAJE VS PROPIEDADES SPA",
        "IS BAST COMPANY SPA",
        "TOKENIT SPA",
        "GENERA S A",
        "FENIX SpA",
        "Bull Capital Servicios Financieros SpA",
        "INMOBILIARIA OASIS DE LA CAMPANA S A",
        "CREDYTSPA",
        "RENDALO CHILE SPA",
        "Xcaler servicios financieros spa",
        "ABC GESTIONA SPA",
        "NOVACARIBE CHILE SPA",
        "IMPORTADORA Y COMERCIALIZADORA CDC S A",
        "MEGAPRO S.A.",
        "ASSET PARTNER SPA",
        "ROMO PROPIEDADES SPA",
        "INVERSIONES RVH SPA",
        "PLUTTO SPA",
        "PUERTO + ARQUITECTURA SPA",
        "Corredores Premium Chile SpA",
        "Inmobiliaria y Constructora Pedro de Valdivia II S.A.",
        "CLA CONSULTORES",
        "INMOBILIARIA VISTA VITACURA SPA",
        "INMOBILIARIA PARQUE LOS VINEDOS SPA",
        "INMOBILIARIA VICUNA MACKENNA 7205 SPA",
        "INMOBILIARIA ISIDORA 2900 SPA",
        "RENTA CORREDORES DE PROPIEDADES LIMITADA",
        "SERVICIO AMBULATORIO NACIONAL DE ATENCIONES DOMICILIARIAS SpA",
        "CLIPERTYSPA",
        "PROPINS SPA",
        "AMIGO FINANCIERO SPA",
        "MARES MARCHANT PROPIEDADES SPA",
        "ISBAST COMPANY SPA",
        "GESTIONES INMOBILIARIAS TUCASA SPA",
        "CASAS CHILE S.A.",
        "INVIERTE PROPIEDADES SPA",
        "MCW INVERSIONES SPA",
        "POLARIS GESTION INMOBILIARIA  SPA",
        "MISABOGADOS PROFESIONALES LIMITADA",
        "AC PLUSVALIA INMOBILIARIA SPA",
        "DISTRIBUIDORA MERCOSUR SPA",
        "Rentas Nueva el Golf Spa",
        "La Esmeralda SpA",
        "ROTHERY & VALDES PROPIEDADES S.P.A",
        "PUERTO ALEGRE SPA",
        "INTERNATIONAL REALTY SPA",
        "AMERIS UPC DEPARTAMENTAL DOS SPA",
        "INMOBILIARIA Y CONSTRUCTORA RADIAL S.A.",
        "INMOBILIARIA ENSENADA PROPIEDADES SPA",
        "Level Propiedades SPA",
        "ADMINISTRACIÓN DE PROPIEDADES CHILE SPA",
        "LAM PROPERTY MANAGEMENT SPA",
        "KVZ GESTION INMOBILIARIA SPA",
        "INVERSIONES Y SERVICIOS TRANSHUARA SPA",
        "SOCIEDAD DE INVERSIONES CASASCHIC SPA",
        "PRO MANAGER INGENIERiA Y CONSTRUCCIoN SPA",
        "MAGDALENA FLORENTINA ISABEL LOPEZ ALFAGEME",
        "PROPIETAT INVERSIONES SPA",
        "Lecaros Desarrollos Inmobilarios Spa",
        "INMOBILIARIA BALMACEDA SPA",
        "GENERA SPA",
        "RENTA CORREDORES DE PROPIEDADES LTDA",
        "ROTHERY VALDES PROPIEDADES SPA",
        "VOD GESTION INMOBILIARIA LDTA",
        "PORTAL GARCIA PROPIEDADESSPA",
        "BROKERS Y NEGOCIOS INMOBILIARIOS DE CHILE LTDA",
        "LATITUD URBANA SPA.",
        "MACARENA RIVERA PALACIOS CORREDOR DE PROPIEDADES EIRL",
        "MI INVERSION PROPIEDADES RODOLFO FABIAN VERA BOBADILLA EIRL",
        "GESTION INMOBILIARIA Y CORRETAJE PROURBE SPA.",
        "DATCAPITAL  ADMINISTRACION  DE  ARRIENDOS  SPA",
    ]
    packs_social_names = [fix_string(item) for item in packs_social_names]

    forcePacks = [549, 509, 429, 381, 223, 382]
    forcePostPago = [398, 356]

    post_social_names = ["LSV SPA", "Corredores Premium Chile SpA"]
    post_social_names = [fix_string(item) for item in post_social_names]

    with alive_bar(len(invoices), force_tty=True) as bar:
        for invoice in invoices:
            invoice_number = invoice[1]
            customer_name = fix_string(invoice[2])
            customer_tax_id = invoice[4]

            if invoice_number in fact_data:
                fact = fact_data[invoice_number]

            # isPack = has_strings(f, [
            #     "pack"])

            if invoice_number in forcePacks or customer_name in packs_social_names:
                print("Update Invoice {}".format(invoice_number))
                update_invoice_mode(fincore_conn, invoice_number, 1)
            else:
                ruts = ["76594721-9",
                        "77769192-9",
                        "76147318-2",
                        "76933833-0",
                        "99520000-7",
                        "77666896-6",
                        "76899853-1",
                        "78029556-2",
                        "76326025-9",
                        "77047877-4",
                        "77088278-8",
                        "77447361-0"
                        ]
                if customer_tax_id in ruts or invoice_number in forcePostPago or customer_name in post_social_names:
                    print("Update Invoice {} to Monthly".format(invoice_number))
                    update_invoice_mode(fincore_conn, invoice_number, 2)

            bar()

    fincore_conn.close()
