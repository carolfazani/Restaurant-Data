from extraction_colibri.temporary_token import GetToken
from extraction_colibri.get_api import make_request

class APIClient:
    def __init__(self):
        """Initializes a client to make requests to the API."""
        self.headers = None

    def authenticate(self):
        """Authenticates the client to obtain an access token."""
        getToken = GetToken()
        token_temp = getToken.get_access_token()
        self.headers = {'Authorization': f'Bearer {token_temp}'}

    def make_api_request(self, url: str, params: dict) -> list:
        """Makes a request to the API.

        Args:
            url (str): The URL of the API endpoint.
            params (dict): The request parameters.

        Returns:
            list: A list containing the response data from the request.
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
            print(f"Request error: {e}")
            return []


class SalesExtractor(APIClient):
    def __init__(self, endpoint: str):
        """Initializes a sales extractor for a specific endpoint.

        Args:
            endpoint (str): The API endpoint.
        """

        super().__init__()
        self.endpoint = endpoint

    def set_params(self, dt_inicio: str, dt_fim: str, lojas: list, redes=None, cancelados=False) -> 'SalesExtractor':

        """Defines parameters for sales extraction.

        Args:
            dt_inicio (str): The start date in 'YYYY-MM-DD' format.
            dt_fim (str): The end date in 'YYYY-MM-DD' format.
            lojas (list): The list of store IDs.
            redes (list): The list of network IDs (optional).
            cancelados (bool): Indicates whether canceled items should be included (default False).

        Returns:
            SalesExtractor: The instance of the sales extractor itself.
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
        """Extracts sales data using the defined parameters.

        Returns:
            list: A list containing the sales data.
        """
        if not self.headers:
            self.authenticate()
        url = f"https://cloud.ncrcolibri.com.br/api/v1/{self.endpoint}"
        return self.make_api_request(url, self.params)


