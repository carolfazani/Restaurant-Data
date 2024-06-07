from extraction_colibri.temporary_token import get_access_token
from extraction_colibri.get_api import make_request

class APIClient:
    def __init__(self):
        """Inicializa um cliente para fazer requisições à API."""
        self.headers = None

    def authenticate(self):
        """Autentica o cliente para obter um token de acesso."""
        token_temp = get_access_token()
        self.headers = {'Authorization': f'Bearer {token_temp}'}

    def make_api_request(self, url: str, params: dict) -> list:
        """Faz uma requisição à API.

        Args:
            url (str): A URL do endpoint da API.
            params (dict): Os parâmetros da requisição.

        Returns:
            list: Uma lista contendo os dados da resposta da requisição.
        """
        try:
            data = make_request(url, self.headers, params)
            total_paginas = data['totalPaginas']
            all_data = [data]

            for pagina in range(2, total_paginas + 1):
                params['pagina'] = pagina
                data = make_request(url, self.headers, params)
                all_data.append(data)

            return all_data

        except Exception as e:
            print(f"Erro na requisição: {e}")
            return []


class SalesExtractor(APIClient):
    def __init__(self, endpoint: str):
        """Inicializa um extrator de vendas para um endpoint específico.

        Args:
            endpoint (str): O endpoint da API.
        """
        super().__init__()
        self.endpoint = endpoint

    def set_params(self, dt_inicio: str, dt_fim: str, lojas: list, redes=None, cancelados=False) -> 'SalesExtractor':
        """Define os parâmetros para a extração de vendas.

        Args:
            dt_inicio (str): A data de início no formato 'YYYY-MM-DD'.
            dt_fim (str): A data de término no formato 'YYYY-MM-DD'.
            lojas (list): A lista de IDs das lojas.
            redes (list): A lista de IDs das redes (opcional).
            cancelados (bool): Indica se os itens cancelados devem ser incluídos (padrão False).

        Returns:
            SalesExtractor: A própria instância do extrator de vendas.
        """
        self.params = {
            'redes': redes,
            'lojas': lojas,
            'dtinicio': dt_inicio,
            'dtfim': dt_fim,
            'cancelados': cancelados,
            'pagina': 1
        }

        return self

    def extract(self) -> list:
        """Extrai dados de vendas usando os parâmetros definidos.

        Returns:
            list: Uma lista contendo os dados de vendas.
        """
        if not self.headers:
            self.authenticate()

        url = f"https://cloud.ncrcolibri.com.br/api/v1/{self.endpoint}"
        return self.make_api_request(url, self.params)


