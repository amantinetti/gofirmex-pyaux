import requests
import json
import csv

url = "https://api.mailgun.net/v3/mg.gofirmex.cloud/messages"
headers = {
    'Authorization': 'Basic YXBpOmY1NTM2MDBlN2U2OTQxYWQzODk1YzhmZTk5YjgwYWViLWZlOWNmMGE4LTFlODI0NGJm'
}

if __name__ == '__main__':

    with open('users.csv', newline='') as csvfile:
        spamreader = csv.reader(csvfile, delimiter=',', quotechar='"')
        for row in spamreader:
            print(', '.join(row))

            variables = {
                "user_email": row[1],
                "user_name": row[0],
                "user_password": row[2]
            }

            payload = {'from': 'no-reply@mg.gofirmex.cloud',
                       'to': variables['user_email'],
                       'template': 'enterprise-welcome',  ## enterprise-welcome / enterprise-user-restore
                       't:variables': json.dumps(variables)
                       }

            response = requests.request("POST", url, headers=headers, data=payload)
            print(response.text)
