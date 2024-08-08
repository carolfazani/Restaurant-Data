# data_extractor.py

# Data Extraction Classes

This module contains classes for interacting with an API to extract sales data. The main classes are `APIClient` for making authenticated requests to the API and `SalesExtractor` for extracting sales data with specific parameters.

## Function `make_request(url, headers, params=None) -> dict`

```python
def make_request(url, headers, params=None) -> dict:
    try:
        response = requests.get(url, headers=headers, params=params)
        if response.ok:
            data = response.json()
            return data
        else:
            print('Request error:', response.status_code)
    except requests.exceptions.RequestException as e:
        print('Request error:', e)
```

Makes a GET request to a specific URL with the provided headers and parameters.

### Parameters
- `url` (str): The URL of the API endpoint.
- `headers` (dict): The request headers.
- `params` (dict, optional): The request parameters. Defaults to None.

### Returns
- `dict`: A dictionary containing the response data in JSON format.

## Class `APIClient`

### Method `__init__()`

```python
def __init__(self):
    self.headers = None
```

Initializes the client without authentication headers.

### Method `authenticate()`

```python
def authenticate(self):
    getToken = GetToken()
    token_temp = getToken.get_access_token()
    self.headers = {'Authorization': f'Bearer {token_temp}'}
```

Authenticates the client to obtain an access token.

### Method `make_api_request(url: str, params: dict) -> list`

```python
def make_api_request(self, url: str, params: dict) -> list:
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
```

Makes a request to the API.

### Parameters
- `url` (str): The URL of the API endpoint.
- `params` (dict): The request parameters.

### Returns
- `list`: A list containing the response data from the request.

## Class `SalesExtractor`

### Method `__init__(endpoint: str)`

```python
def __init__(self, endpoint: str):
    super().__init__()
    self.endpoint = endpoint
```

Initializes a sales extractor for a specific endpoint.

### Parameters
- `endpoint` (str): The API endpoint.

### Method `set_params(dt_inicio: str, dt_fim: str, lojas: list, redes=None, cancelados=False) -> 'SalesExtractor'`

```python
def set_params(self, dt_inicio: str, dt_fim: str, lojas: list, redes=None, cancelados=False) -> 'SalesExtractor':
    self.params = {
        'redes': redes,
        'lojas': lojas,
        'dtinicio': dt_inicio,
        'dtfim': dt_fim,
        'cancelados': cancelados,
        'pagina': 1
    }

    return self
```

Defines parameters for sales extraction.

### Parameters
- `dt_inicio` (str): The start date in 'YYYY-MM-DD' format.
- `dt_fim` (str): The end date in 'YYYY-MM-DD' format.
- `lojas` (list): The list of store IDs.
- `redes` (list, optional): The list of network IDs (default is None).
- `cancelados` (bool): Indicates whether canceled items should be included (default is False).

### Returns
- `SalesExtractor`: The instance of the sales extractor itself.

### Method `extract() -> list`

```python
def extract(self) -> list:
    if not self.headers:
        self.authenticate()
    url = f"https://cloud.ncrcolibri.com.br/api/v1/{self.endpoint}"
    return self.make_api_request(url, self.params)
```

Extracts sales data using the defined parameters.

### Parameters
None

### Returns
- `list`: A list containing the sales data.


Here is the updated documentation for the provided `GetToken` class in English:

---
temporary_token.py

## Class `GetToken` 

The `GetToken` class is designed to manage and retrieve an access token from Google Cloud's Secret Manager. It fetches the token, checks for expiration, and renews it as needed.

### Parameters
- `project_id` (str): The Google Cloud project ID, retrieved from the environment variable `etl_project_id`.
- `secret_name` (str): The name of the secret in Google Cloud's Secret Manager, retrieved from the environment variable `SECRET_NAME`.
- `secret_version` (str): The version of the secret in Google Cloud's Secret Manager, retrieved from the environment variable `SECRET_VERSION`.
- `client` (SecretManagerServiceClient): A client for accessing Google Cloud's Secret Manager.
- `access_token` (str): The current access token.
- `token_expiration` (datetime): The expiration time of the current access token.
- `secret_payload` (str): The payload of the secret, containing the necessary credentials for authentication.




### Method `__init__(self)`
Initializes the `GetToken` instance. It sets the project ID, secret name, and secret version from environment variables. It also initializes the Secret Manager client and retrieves the secret payload.

```python
def __init__(self):
    self.project_id = os.environ.get("etl_project_id")
    self.secret_name = os.environ.get("SECRET_NAME")
    self.secret_version = os.environ.get("SECRET_VERSION")
    self.client = secretmanager.SecretManagerServiceClient()
    self.access_token = None
    self.token_expiration = None
    self.secret_payload = self._get_secret()
```

### Method `_get_secret(self)`
Fetches the secret payload from Google Cloud's Secret Manager using the project ID, secret name, and secret version.

```python
def _get_secret(self):
    name = f"projects/{self.project_id}/secrets/{self.secret_name}/versions/{self.secret_version}"
    response = self.client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")
```

### Method `_is_token_expired(self)`
Checks if the current access token has expired by comparing the current time with the token expiration time.

```python
def _is_token_expired(self):
    current_time = datetime.datetime.now()
    return self.token_expiration is None or current_time >= self.token_expiration
```

### Method `get_access_token(self)`
Retrieves the access token. If the current token is valid and not expired, it returns the existing token. Otherwise, it fetches a new token by making a request to the authentication URL, sets the new token and its expiration time, and returns the new token.

```python
def get_access_token(self):
    if self.access_token is not None and not self._is_token_expired():
        return self.access_token
    token = self.secret_payload
    url = f'https://cloud.ncrcolibri.com.br/oauth/authenticate?client_id={token}'
    headers = {
        'Authorization': f'Bearer {token}'
    }
    data = make_request(url, headers)
    self.access_token = data['access_token']
    self.token_expiration = datetime.datetime.now() + datetime.timedelta(minutes=5)

    return self.access_token
```
