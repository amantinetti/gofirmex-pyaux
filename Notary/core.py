import base64

import requests
import json
import psycopg2
import urllib.request

# files_manager_url = "http://10.142.0.16/ms/files-manager/v1"
files_manager_url = "https://files-manager.cloud-run.gofirmex.cloud"


def file_to_base64(filepath):
    with open(filepath, 'rb') as binary_file:
        binary_file_data = binary_file.read()
        base64_encoded_data = base64.b64encode(binary_file_data)
        base64_message = base64_encoded_data.decode('utf-8')

    return base64_message
