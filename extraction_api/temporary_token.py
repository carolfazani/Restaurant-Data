from extraction_api.api import make_request
import os

def get_access_token():
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
    token_temp = data['access_token']

    return token_temp
