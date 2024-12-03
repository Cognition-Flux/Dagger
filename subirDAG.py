import sys
import requests


def upload_file(file_path):
    # URL del endpoint
    url = "https://4fruwh2zzb.execute-api.us-east-1.amazonaws.com/Prod/upload/"

    # Extraer el nombre del archivo de la ruta
    file_name = file_path.split("/")[-1]

    # Parámetros de consulta (query parameters)
    params = {"filename": file_name}

    # Leer el contenido del archivo en modo binario
    with open(file_path, "rb") as file:
        file_data = file.read()

    # Encabezados de la solicitud
    headers = {"Content-Type": "application/octet-stream"}

    # Enviar la solicitud POST
    response = requests.post(url, params=params, data=file_data, headers=headers)

    # Imprimir la respuesta
    print("state:", response.status_code)
    print("body:", response.text)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Uso: python upload_script.py <ruta_del_archivo>")
        sys.exit(1)
    ruta_del_archivo = sys.argv[1]
    upload_file(ruta_del_archivo)
