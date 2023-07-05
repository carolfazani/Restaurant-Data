from extraction_colibri.temporary_token import get_access_token
from extraction_colibri.colibri_api import make_request
import json


def revenue_extraction(dt_inicio, dt_fim):
    token_temp = get_access_token()


    # Cabeçalho da requisição
    headers = {
        'Authorization': f'Bearer {token_temp}'
    }

    #Parâmetros da requisição
    params = {
        'redes': 1422358772144,  # ID(s) da(s) rede(s)
        'lojas': 1504187988489, # ID(s) da(s) loja(s)
        'dtinicio': dt_inicio, #2017-08-30'
        'dtfim': dt_fim,
        'pagina': 1

    }

    all_data = []  # Lista para armazenar todos os dados

    while True:
        url = f"https://cloud.ncrcolibri.com.br/api/v1/movimentocaixa?dtinicio={params['dtinicio']}&dtfim={params['dtfim']}&pagina={params['pagina']}"

        try:
            # Fazer a requisição para a página atual
            data = make_request(url, headers, params)

            if not data:
                break  # Se não houver mais dados, sair do loop

            all_data.append(data)  # Adicionar os dados na lista

            params['pagina'] += 1  # Incrementar a página para a próxima requisição

        except Exception as e:
            # Tratar o erro de requisição 400
            print(f"Erro na requisição: {e}")
            break

    return all_data

