import pandas as pd
from database.database_connection import MysqlPython
from extraction_sheets.sheets_api import read_google_sheets
from datetime import datetime


def insert_recebimentos():
    mysql_python = MysqlPython()
    dados_sheets = read_google_sheets()

    sql_produtos = "INSERT IGNORE INTO Produtos (nome_produto, unidade) VALUES (%s, %s)"
    sql_estoque = "INSERT IGNORE INTO Estoque (nome_produto, data, quantidade) VALUES (%s, %s, %s)"

    values_produtos = []
    values_estoque = []

    # Iterar sobre as planilhas e acessar os DataFrames
    for nome_planilha, df in dados_sheets.items():
        if nome_planilha == "Dicionario":
            df = df.dropna()
            df_dicionario = df
            for _, row in df_dicionario.iterrows():
                values_produtos.append((row['produto'], row['unidade_medida']))
        else:
            # Converter colunas de datas em linhas
            df_melted = pd.melt(df, id_vars=['produto', 'unidade_medida'], var_name='data', value_name='quantidade')
            df_melted = df_melted.dropna()
            for _, row in df_melted.iterrows():
                data_obj = datetime.strptime(row['data'], '%m/%d/%Y')
                data_formatada = data_obj.strftime('%Y-%m-%d')
                if row['unidade_medida'] != 'unidade_medida' and row['quantidade'] != '':
                    values_estoque.append((row['produto'], data_formatada, row['quantidade']))

    mysql_python.query(sql_produtos, params=values_produtos, commit=True)
    mysql_python.query(sql_estoque, params=values_estoque, commit=True)


insert_recebimentos()