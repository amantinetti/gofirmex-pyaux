from Crypto.Cipher import AES
import os
import base64


def encrypt(plaintext_bytes: bytes, key_string: str):
    try:
        # Convertir el string de la clave a bytes
        key = key_string.encode('utf-8')

        # Crear un nonce de 12 bytes (estándar GCM)
        nonce = os.urandom(12)

        # Crear el cipher AES-GCM
        cipher = AES.new(key, AES.MODE_GCM, nonce=nonce)

        # Encriptar y obtener el tag de autenticación
        ciphertext, tag = cipher.encrypt_and_digest(plaintext_bytes)

        # Concatenar nonce + ciphertext + tag
        encrypted = nonce + ciphertext + tag

        # Codificar en base64 para enviar como string
        encrypted_b64 = base64.b64encode(encrypted).decode('utf-8')

        return encrypted_b64

    except Exception as e:
        raise e


if __name__ == '__main__':
    key = "tu-clave-de-32-bytes-exactamente"
    filepath = "./documents/document_ripley.pdf"
    with open(filepath, 'rb') as binary_file:
        binary_file_data = binary_file.read()
        base64_message = encrypt(binary_file_data, key)

        with open("encrypted.txt", 'w', encoding='utf-8') as file:
            file.write(base64_message)