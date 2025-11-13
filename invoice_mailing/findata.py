import os
import sys
import json
import argparse
import xml.etree.ElementTree as ET
import csv
from typing import List, Dict, Tuple

NS = {"sii": "http://www.sii.cl/SiiDte"}


def load_json(path: str):
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, list):
            raise ValueError("El JSON raíz debe ser una lista de objetos.")

        org_data = {}
        for item in data:
            org_data[item['tin']] = item['id']

        return org_data
    except Exception as e:
        sys.stderr.write(f"[ERROR] No se pudo leer/parsing el JSON: {e}\n")
        sys.exit(1)


def extract_receptor_fields(xml_path):
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

        def txt(tag):
            el = receptor.find(f"sii:{tag}", NS)
            return el.text.strip() if el is not None and el.text else None

        entry = {
            "RUTRecep": txt("RUTRecep"),
            "RznSocRecep": txt("RznSocRecep"),
            "GiroRecep": txt("GiroRecep"),
            "DirRecep": txt("DirRecep"),
            "CmnaRecep": txt("CmnaRecep"),
        }

        # Solo agregar si hay al menos el RUT o razón social
        if any(entry.values()):
            resultados.append(entry)

    return resultados


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


def write_csv(pairs: List[Tuple[str, str]], path: str = None):
    if path:
        f = open(path, "w", encoding="utf-8", newline="")
        close = True
    else:
        f = sys.stdout
        close = False
    try:
        writer = csv.writer(f)
        writer.writerow(["tin", "id"])
        for tin, _id in pairs:
            writer.writerow([tin, _id])
    finally:
        if close:
            f.close()


def main():
    parser = argparse.ArgumentParser(
        description="Extrae RUTRecep, RznSocRecep, DirRecep, CmnaRecep desde XML de facturas SII y genera JSON."
    )
    parser.add_argument(
        "-i", "--input",
        help="Ruta a un archivo XML o a una carpeta que contenga XML.",
        default="./facturas"
    )
    parser.add_argument(
        "-o", "--output",
        default="./accounts.json",
        help="Archivo de salida. Si no se indica, se imprime por stdout."
    )

    parser.add_argument(
        "-f", "--format",
        choices=["text", "csv", "json"],
        default="text",
        help="Formato de salida: text | csv | json (por defecto: csv)."
    )

    args = parser.parse_args()

    files = gather_xml_files(args.input)
    all_rows = []
    for f in files:
        rows = extract_receptor_fields(f)
        all_rows.extend(rows)

    data = {}

    orgs = load_json("./org.json")

    for row in all_rows:
        if row['RUTRecep'] in data:
            if row['RznSocRecep'].find(data[row['RUTRecep']]['RznSocRecep']) and row[
                'RznSocRecep'] != "Sin información":
                data[row['RUTRecep']]['RznSocRecep'] = f"{data[row['RUTRecep']]['RznSocRecep']} - {row['RznSocRecep']}"

            if row['GiroRecep'].find(data[row['RUTRecep']]['GiroRecep']) and row['GiroRecep'] != "Sin información":
                data[row['RUTRecep']]['GiroRecep'] = f"{data[row['RUTRecep']]['GiroRecep']} - {row['GiroRecep']}"

            if row['DirRecep'].find(data[row['RUTRecep']]['DirRecep']) and row['DirRecep'] != "Sin información":
                data[row['RUTRecep']]['DirRecep'] = f"{data[row['RUTRecep']]['DirRecep']} - {row['DirRecep']}"

            if row['CmnaRecep'].find(data[row['RUTRecep']]['CmnaRecep']) and row['CmnaRecep'] != "Sin información":
                data[row['RUTRecep']]['CmnaRecep'] = f"{data[row['RUTRecep']]['CmnaRecep']} - {row['CmnaRecep']}"
        else:
            if row['RznSocRecep'] == "Sin información":
                row['RznSocRecep'] = ""
            if row['GiroRecep'] == "Sin información":
                row['GiroRecep'] = ""
            if row['DirRecep'] == "Sin información":
                row['DirRecep'] = ""
            if row['CmnaRecep'] == "Sin información":
                row['CmnaRecep'] = ""
            data[row['RUTRecep']] = row

            if row['RUTRecep'] in orgs:
                data[row['RUTRecep']]['organization'] = orgs[row['RUTRecep']]

    fixdata = []

    for key in data:
        fixdata.append(data[key])

    if args.format == "csv":
        write_csv(fixdata, args.output)
    else:
        if args.output:
            with open(args.output, "w", encoding="utf-8") as out:
                json.dump(fixdata, out, ensure_ascii=False, indent=2)
        else:
            print(json.dumps(fixdata, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
