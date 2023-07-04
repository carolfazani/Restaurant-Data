import gspread
from oauth2client.service_account import ServiceAccountCredentials
from extraction_sheets.api_keys import create_keyfile_dict
import pandas as pd

def read_google_sheet():
    # Defina o escopo e as credenciais do Google Sheets
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    credentials = creds = ServiceAccountCredentials.from_json_keyfile_dict(create_keyfile_dict(), scope)


    # Autorize e abra a planilha do Google Sheets
    client = gspread.authorize(credentials)
    planilha = client.open('estoque_entradas')
    folhas = planilha.worksheets()

    # Extrair cada folha da planilha para um DataFrame
    dados_sheets = {}
    for folha in folhas:
        nome_folha = folha.title
        dados = folha.get_all_records()
        df = pd.DataFrame(dados)
        dados_sheets[nome_folha] = df

    return dados_sheets
