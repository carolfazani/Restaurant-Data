from extraction_colibri.temporary_token import get_access_token
from extraction_colibri.get_api import make_request

class DataExtractor:
    def __init__(self):
        """Inicializa uma instância da classe APIFacade."""
        self.token = None
        self.headers = None
        self.base_url = "https://cloud.ncrcolibri.com.br/api/v1/"

    def authenticate(self):
        """Autentica o cliente para obter um token de acesso."""
        self.token = get_access_token()
        self.headers = {'Authorization': f'Bearer {self.token}'}

    def make_api_request(self, endpoint: str, params: dict):
        """Faz uma requisição para o endpoint especificado.

        Args:
            endpoint (str): O endpoint da API.
            params (dict): Os parâmetros da requisição.

        Returns:
            dict: Os dados da resposta da requisição.
        """
        url = f"{self.base_url}{endpoint}"
        try:
            return make_request(url, self.headers, params)
        except Exception as e:
            print(f"Erro na requisição para {url}: {e}")
            return None

    def item_sales_extraction(self, dt_inicio: str, dt_fim: str):
        """Extrai dados de vendas de itens de um período específico.

        Args:
            dt_inicio (str): A data de início do período no formato 'YYYY-MM-DD'.
            dt_fim (str): A data de término do período no formato 'YYYY-MM-DD'.
            lojas (list): A lista de IDs das lojas. O padrão é [1].

        Returns:
            list: Uma lista contendo os dados de vendas de itens.
        """
        self.authenticate()
        params = {
            'redes': 1422358772144,  # ID(s) da(s) rede(s)
            'lojas': 1504187988489,  # ID(s) da(s) loja(s)
            'dtinicio': dt_inicio,  # '2017-08-30'
            'dtfim': dt_fim,
            'cancelados': False,
            'pagina': 1
        }

        all_data = []

        endpoint = "itemvenda"
        try:
            data = self.make_api_request(endpoint, params)
            total_paginas = data['totalPaginas']
        except Exception as e:
            print(f"Erro na requisição: {e}")
            return all_data

        for pagina in range(1, total_paginas + 1):
            params['pagina'] = pagina
            data = self.make_api_request(endpoint, params)

            if not data:
                break  # Se não houver mais dados, sair do loop

            all_data.append(data)

        return all_data


# Utilização da fachada
api = DataExtractor()
data = api.item_sales_extraction("2024-01-01", "2024-01-31")
print(data)
