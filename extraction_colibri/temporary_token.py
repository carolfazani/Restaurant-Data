from extraction_colibri.colibri_api import make_request
import os
import datetime

# Variáveis globais para armazenar o token e sua data de expiração
ACCESS_TOKEN = None
TOKEN_EXPIRATION = None

# Verificar se o token está expirado
def is_token_expired():
    global TOKEN_EXPIRATION
    current_time = datetime.datetime.now()
    return TOKEN_EXPIRATION is None or current_time >= TOKEN_EXPIRATION

# Função para obter o token de acesso
def get_access_token():
    global ACCESS_TOKEN, TOKEN_EXPIRATION

    # Verificar se o token já foi obtido e está válido
    if ACCESS_TOKEN is not None and not is_token_expired():
        return ACCESS_TOKEN

    # Token de acesso
    token = os.environ.get('TOKEN_ACESSO')

    # Endpoint da API
    url = f'https://cloud.ncrcolibri.com.br/oauth/authenticate?client_id={token}'

    # Cabeçalho da requisição
    headers = {
        'Authorization': f'Bearer {token}'
    }

    # Fazer a requisição e obter o access_token
    data = make_request(url, headers)
    ACCESS_TOKEN = data['access_token']
    TOKEN_EXPIRATION = datetime.datetime.now() + datetime.timedelta(minutes=5)



    return ACCESS_TOKEN

get_access_token()
