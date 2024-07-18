import datetime
import os
from google.cloud import secretmanager
from extraction_colibri.get_api import make_request

class GetToken:
    def __init__(self):
        self.project_id = '572991705520' #os.environ.get("etl_project_id")
        self.secret_name = os.environ.get("SECRET_NAME")
        self.secret_version = os.environ.get("SECRET_VERSION")
        self.client = secretmanager.SecretManagerServiceClient()
        self.access_token = None
        self.token_expiration = None
        self.secret_payload = self._get_secret()

    def _get_secret(self):
        name = f"projects/{self.project_id}/secrets/{self.secret_name}/versions/{self.secret_version}"
        response = self.client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")

    def _is_token_expired(self):
        current_time = datetime.datetime.now()
        return self.token_expiration is None or current_time >= self.token_expiration

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


