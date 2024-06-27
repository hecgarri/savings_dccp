import requests

url = "https://api.cne.cl/api/login"
email = "hectorgarridohenriquez@gmail.com"
password = "r2ikSGpgwetgjAk"

def get_auth_token(email, password):
    url = "https://api.cne.cl/api/login"
    response = requests.post(
        url,
        json={"email": email, "password": password}
    )
    return response.json()["token"]

token = get_auth_token(email, password)

# Función para hacer una solicitud autenticada utilizando el token
def make_authenticated_request(token, endpoint):
    url = "https://api.cne.cl" + endpoint
    response = requests.get(
        url,
        headers={"Authorization": "Bearer " + token}
    )
    return response.json()

data = make_authenticated_request(token, "/api/v4/estaciones")
