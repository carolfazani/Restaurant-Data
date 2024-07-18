import requests

def make_request(url, headers, params=None) -> dict:
    """
    Makes a GET request to a specific URL with the provided headers and parameters.

    Args:
        url (str): The URL of the API endpoint.
        headers (dict): The request headers.
        params (dict, optional): The request parameters. Defaults to None.

    Returns:
        dict: A dictionary containing the response data in JSON format.
    """
    try:
        response = requests.get(url, headers=headers, params=params)
        if response.ok:
            data = response.json()
            return data
        else:
            print('Request error:', response.status_code)
    except requests.exceptions.RequestException as e:
        print('Request error:', e)