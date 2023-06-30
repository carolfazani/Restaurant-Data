import requests
def make_request(url, headers, params=None):
    try:
        # Fazendo a requisição GET
        response = requests.get(url, headers=headers, params=params)

        # Verificando se a requisição foi bem-sucedida (código de status 200)
        if response.status_code == 200:
            # Acessando os dados da resposta (formato JSON)
            data = response.json()
            return data
        else:
            # Caso a requisição não tenha sido bem-sucedida, trate o erro de acordo
            print('Erro na requisição:', response.status_code)

    except requests.exceptions.RequestException as e:
        # Caso ocorra um erro na requisição, trate a exceção
        print('Erro na requisição:', e)