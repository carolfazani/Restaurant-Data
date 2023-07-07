from extraction_colibri.temporary_token import get_access_token
from extraction_colibri.get_api import make_request

def item_sales_extraction(dt_inicio, dt_fim):
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
        'cancelados': False,
        'pagina': 1

    }

    all_data = []  # Lista para armazenar todos os dados

    url = f"https://cloud.ncrcolibri.com.br/api/v1/itemvenda?lojas={params['lojas']}&dtinicio={params['dtinicio']}&dtfim={params['dtfim']}&pagina=1"
    try:
        data = make_request(url, headers, params)
        total_paginas = data['totalPaginas']
    except Exception as e:
        print(f"Erro na requisição: {e}")
        return all_data

    # Iterar sobre as páginas utilizando um loop for
    for pagina in range(1, total_paginas + 1):
        url = f"https://cloud.ncrcolibri.com.br/api/v1/itemvenda?lojas={params['lojas']}&dtinicio={params['dtinicio']}&dtfim={params['dtfim']}&pagina={pagina}"
        try:
            # Fazer a requisição para a página atual
            data = make_request(url, headers, params)

            if not data:
                break  # Se não houver mais dados, sair do loop

            all_data.append(data)  # Adicionar os dados na lista

        except Exception as e:
            # Tratar o erro de requisição
            print(f"Erro na requisição: {e}")
            break

    return all_data