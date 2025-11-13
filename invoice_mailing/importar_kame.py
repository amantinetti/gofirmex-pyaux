import requests
import glob


def sendInvoiceToKame(invoice_file, type="V"):  # Type puede ser V de venta o C de compra

    headers = {
        'sec-ch-ua-platform': '"macOS"',
        'Cache-Control': 'no-cache',
        'sec-ch-ua': '"Chromium";v="142", "Google Chrome";v="142", "Not_A Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'X-Requested-With': 'XMLHttpRequest',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36',
        'Accept': 'application/json',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Dest': 'empty',
        'host': 'www.kameone.cl'
    }

    headers = {
        'sec-ch-ua-platform': '"macOS"',
        'Cache-Control': 'no-cache',
        'sec-ch-ua': '"Not;A=Brand";v="99", "Google Chrome";v="139", "Chromium";v="139"',
        'sec-ch-ua-mobile': '?0',
        'X-Requested-With': 'XMLHttpRequest',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
        'Accept': 'application/json',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Dest': 'empty',
        'host': 'www.kameone.cl',
        'cookie': 'nombreUsuario=%2flQmB8WVNnvpAh6375x5oeNeaY%2bBQ%2biA; skinUsuario=kame.css; skinUsaModoNocturno=N; ID_Usuario=HPg2%2fbCLIs4%3d; emailUsuario=RzxqtA8gbCAOlfZWzVArF3IM%2boNibX5d; empresas=AL6V7eaxfQg%3d; __RequestVerificationToken=Yilsg5ty-pbBePs3azZgHl69HNR0pzNT6C5e72-3wNP4isLxbuyFjNe1eZjpBp9heAfDK4aSL4ptKF-0Naqt393LHM1iUXZtDKxJ1PUDypk1; admin=73dQLXpmNNw%3d; tipoPlan=QFKArBA5jJ5RtpNvznSJXw%3d%3d; verPlanContratado=73dQLXpmNNw%3d; verOpcionesAvanzadas=73dQLXpmNNw%3d; _ga=GA1.1.190762653.1762378964; intercom-device-id-e21ouv36=5c41806a-11df-4b4f-a2e9-6599318d116d; recaptcha-ca-t=AUUJSe6u2FZj0sgoxdzz1KRzzbW9hIU0Uh6bTh5SLny2V4l0fmvrK4_EqZg7pFcWwOyEIkLNXyWz3KdMOTdUx5QOXyEM1kjexBR9qeYv2gFKPVYXXj7eYs5awo8sVqKEWYG_rUgwgsYXlaKbOIuNOHzsk1GwUOMfbObOK26QMBYMBwzY--FRPdf_exOT0SY4m_MZRm8Lx2eIVaQf:U=6f22f59e09000000; timeStampSignUp=1762531659681; token=YrPmLhYtmxkHg5e3pgfVeROni3ZixjVh%2fy7uj%2fNQmkASXSRCBodPE6rMRgT4PD4jAK%2bXnnOSK4qAswqudOnXvsIfwIgD%2fVe7YOqrTPy2lEe06TWQBEJ27zCKPqDIABk8dsswJww4Q7biEv7Wf%2fZyyiqYDj%2faxFiJHinicqA8R%2fnobdGS6vxJGkmucXkEwfWIrwIriJjP8I4F9LBYbwqfeY%2bfXWVgBEJ9DFiTSXwEyV728LAwGDkW%2b6jS24E2W38NrCoTPlc4TG5Y%2baIB4OQywrURKrVYp%2fi4N7DHkdQuCQo3TkeiZlFazPGQbTpy3ntFS0KRkPwJOncTW2N9UnX8O5CYvvXG0dEt%2f6Yc1dHVHxKzAQoFrzKt3rPV6LrpXFbl0Tegz6YMx4%2bpcK6MSUGd1DLk1GpvJd9CfLvwqaUmlOfgdP5Udc%2btrEV96p0nERWia5B2GNSelegmOQtfzarFTTUowA0gBV%2fDHb3YajpK4%2fjspl1fbWjemV0PUxFGDUQ2Rg4tyRMwXgJO7MP4KmruxaLrPp6wFk7n%2frSKLxl3ecYYTEIZ9ylPv8QwZR650MZikYhXbjRtWqn%2bKLxSD5YQnFtoKoQx9RKyMPcixW%2bclgtoq%2fVjCcf83yRqALZnyGqNoCIxkwy2M%2bPVfGRdoaCknx27YA8hZ4nou5hHuuUbPobbHrlkodLwZ%2f6guq6fGfvm6tUrIUB9OwGrx%2fGQ8YC4vKouSvYeXzswyRfYgfaErJxx8w9pByNLCOWNMoIIn9OSI64mHclkeNJYa8%2f5hibb8c%2bebDFUKk3%2fTFfdnGPMjpv3ge%2ffjXsY9d%2fNUWigzTGNF6j0Se0o8tefp2vVC%2fayzt7y8fjdgZEsTqGdVJkQ7iEvNfAuXFb7t25kL8y2MIMvXP%2fFHJReC5hXhYJluLPafaoypCDs2eeYDk4bne0f6wParMRKgw3WajyazTjRx7EL%2foXyh4LhKsuHimJfWrjWPfmChQ5ifeQo5f%2b39j838OeFY24zaMKBKC9F4N23Vrtw5LKXH%2f6oII333O58UVDANqP2W6Q7YiTa%2fJC7tkMGTnNy5nY8KVeFGKuJROolUNpAryi3hSL34Jc6diHvN6%2btNlszljbGQ7LHjUtsYtcYj%2b51sjJY8XOsCCiExslGJVm%2bAyWfcpXIFcjf4%2fucqrk9cIcSmGttiPQ77rCc12XUCNeLEc%2fCKuUe0IA%2bef8VLEsittDUQH5yPrDVTQsdhfUs73Qn0zJOKHIKNeYnY0RzCbDRDTDDmA89nPbktag6%2bAzHtK0p83%2fVj5hOL3yfg9U9lYRMCXsBEghm01Ng%2bFS8wtjrBy8ti4ppiVxQmtVAY0bave0YNyjTR6HwIrAUh9QSFKjy%2f4aux6w3t9cSplkG1iL4Cc9hL4anoIT28ryXN5di2NQlgyrPE%2f8%2bDovw5a%2fUXsY1fT1dWT8kNIO2qiFbkqcSFGqswAky%2bOMqR%2fKuIZlVBVdTgCfcHSPcNhkWS3uwvb5pl7o5RbnuljMr79Sb1EtrZ2esb7Y8bP5xfT7VUbqj86wazNsurLje6xbM5M3tC9tmwv0Nktb6CczUM8BXT8JH1jMa1OmZ%2b42knm7AQpDtQJcQ5qi9RBE1SCUgDESkUeGnZGWF2bHyUbmzP%2f1zGYsN%2f5sOunP9Nkzpezex04TMG1%2bX63QE4KPMZAYqz2WFGDpcm53GbboBSAZLGGTNVumI9d03TaOvUZOzLkgwT1hI%2fxVCKwDpEISTQUzBitxTnt49aNdq4mUvxg5ghaUzrsja9lHpci5agXyb6869xOyslDnFH84q3FtS2enS3pLIjFcQ9JGE1wboYgCAQ%2b0DQ2JJBpAaUMRFa3D8Wd8SeY%2b%2b2QDp1QkO3pCLEB4EzoXaLE4ELy2RZZ3J%2f9ipSps1IZm21AmplJeAYrXEIzjBX2U5mHEk9paFt3qs8iIW5C9hI%2bbk8sAJPnNUiS1IBJMb6RoNtnfCednmWe3KVHn1tiNwLlniYmXIVrJXuns8GFvI94pJs2oXXZUhGosAZETFUhhCTspVA0eyWx9irozH6%2bEql47BPQqftn0KdqHyhZXEYSk5PVFX1tUiVeaSw3eOpP4e9Hj%2bP729YwyLPyHXNcnE1mts%2fMljNqRM9VGemVQQ8OadEa4hm8%2ff1O5nxZhlVuAQ220elu%2bDkRKaoCc0XUekU5O3EA2%2fcWhq3AY5J05UwylCb%2bNVu%2bFlZ7%2fEnJQDSHlBl%2bC5cqeOxmXf8%2fWuAvkQ3aihkesinn25LkTbLSfLenJhFmrHXjTDceaIcgueo82j8eEDKaP0az3IzcSnJp%2fxOyqVE%2fuh1vSUF1Pg269kNmfKUXLfwI4u6CHDfiB057VxIJrl%2b9LT8B1VX4H4qAPq9G9XRiRekeNX8d5NgBoXzuDRTgbJqNvmIZX5zABYKKQSz870v%2fZktpBLLVoWvm0p1yWoFffORl4gz8IVYgu6MLylGVXbBDktInwJI6hQ7vCz9yTKhLQtfmZK%2fXmcr1FJV2qjvLfz9p1UaqH%2f11Xk95qb398Jz8lvQvMmli1RWHQZxNedladHDA%3d%3d; ID_Empresa=LQFMXofTBQg%3d; nombreEmpresa=ZKeCqDPGTP7LkfYcNxIc4Q%3d%3d; rutEmpresa=2rCsbPkTMDNymuqs%2f%2fPS6A%3d%3d; sesion=GR1AXFs6Hmto4uZSzmtS1pA6pplubyNSUW3yOttvTv%2bq%2b0Je2vCLfg%3d%3d; _ga_Q0PJVLFH4V=GS2.1.s1762531670$o3$g1$t1762533384$j54$l0$h0; intercom-session-e21ouv36=QW1nZ0F3RGdySGs4VEVBR3JSMUU1bERBdHQwdmNxYW5JY3d0QXlxbFVWSXYxdlF5NHdHQzI1VGdKZnJvTVVHQzRVTWc4TU4zQ25oWGMrUE52TDVaclFKM2lWRnNQSzBNRG1mSW9hcWhRU009LS1wQjRyV1B3RzFxdFlLRGxTUzJlUjlRPT0=--edbe15004c86b6d6a40f601b9ba12e0b3083ed24; __cf_bm=3RjL_SYXur2onOHlaJipePmVeUmsSI_ggah35m4LWD0-1762533397.0487723-1.0.1.1-IscQxkFNvTw_7qCbOi91RvjsRfO0xLZEIGS08WnQG9IkWIwEi1TemblLKu1t_68XUgA1Cj3V8pCsLkbtPb2sIMQQAFHcBZyRQC2pVB48yduYNyGzDtV3EPWQetyxl_cU'
    }

    url = "https://www.kameone.cl/Documento/ImportarPortal"
    form_data = {
        "selUNegocioForm": "67239",
        "PeriodoTributarioForm": "09/2025",
        "PeriodoSIIForm": "09/2025",
        "chkGuiasForm": "N1234",
        "chkNCNoRefForm": "N",
        "chkExcluirBoletasForm": "N",
        "sincSII": "",
        "tipoDoc": type,
        "sincBSale": "",
    }
    resp = requests.post(url, headers=headers, data=form_data, files={"file": open(invoice_file, "rb")})
    return resp.json()


def process(files, type):
    for f in files:
        print("Send {}".format(f))
        r = sendInvoiceToKame(f, type)
        if r['resultadoError'] != "":
            print("\tERROR: {}".format(r['resultadoError']))
        else:
            print("\tOK")


if __name__ == '__main__':
    print("Send Invoices to Kame ERP")

    print("----- Facturas -------")
    facturas = sorted(glob.glob("facturas/**/*.xml", recursive=True), key=str.lower, reverse=True)
    process(facturas, "V")

    print("\n")
    print("----- Boletas -------")
    boletas = sorted(glob.glob("boletas/**/*.xml", recursive=True), key=str.lower, reverse=True)
    process(boletas, "V")

    print("\n")
    print("----- Notas de Credito -------")
    nc = sorted(glob.glob("nc/**/*.xml", recursive=True), key=str.lower, reverse=True)
    process(nc, "V")

    print("\n")
    print("----- Compras -------")
    compras = sorted(glob.glob("compras/**/*.xml", recursive=True), key=str.lower, reverse=True)
    process(compras, "C")
