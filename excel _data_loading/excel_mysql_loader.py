import pandas as pd
from datetime import datetime
from database.database_connection import MysqlPython

def insert_recebimentos():
    mysql_python = MysqlPython()
    # Caminho para o arquivo do Excel
    caminho_arquivo = 'C:\\Users\\conta\\colibri_data\\estoque_entradas.xlsx'

    # Carregar todas as planilhas do arquivo
    tabelas_excel = pd.read_excel(caminho_arquivo, sheet_name=None)

    sql_produtos = f'''INSERT IGNORE INTO Produtos (nome_produto, unidade) VALUES'''
    sql_estoque = f'''INSERT IGNORE INTO Estoque (nome_produto, data, quantidade) VALUES'''

    # Iterar sobre as planilhas e acessar os DataFrames
    for nome_planilha, df in tabelas_excel.items():
        if nome_planilha == "Dicionário":
            df_dicionario = df
            df = df.dropna()
            for _, row in df_dicionario.iterrows():
                values_produtos = f'''(
                '{row['produto']}',
                '{row['unidade_medida']}')'''

                sql_produtos += values_produtos + ","
        else:
            # Converter colunas de datas em linhas
            df_melted = pd.melt(df, id_vars=['produto','unidade_medida'], var_name='data', value_name='quantidade')
            # Remover linhas com valores ausentes
            df_melted = df_melted.dropna()
            for _, row in df_melted.iterrows():
                values_estoque = f'''(
                '{row['produto']}',
                '{row['data']}',
                {row['quantidade']})'''

                sql_estoque += values_estoque + ","

    sql_produtos = sql_produtos[:-1]
    sql_estoque = sql_estoque[:-1]


    mysql_python.query(sql_produtos, commit=True)
    mysql_python.query(sql_estoque, commit=True)


insert_recebimentos()