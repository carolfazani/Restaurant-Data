from transform_data.data_converter import *
from database.bigQuery import BigQueryPython
from datetime import datetime
import pandas as pd
import os


class GoldModelData:
    def __init__(self, project_id: str, date: str):
        self.project_id = project_id
        self.bq = BigQueryPython(project=project_id)
        self.date = date


    def extract_item_sales(self) -> pd.DataFrame:
        """Extrai as vendas de itens da tabela Bronze e transforma os dados."""
        query = f""" SELECT * FROM `colibri-413121.silver.item_sales` WHERE extraction_date = '{self.date}' """
        print(query)
        # Executar a consulta
        query_job = self.bq.query(query)
        print(query_job)
        item_sales_list = []
        for item_sales in query_job:
            item_sales_dict = dict(item_sales)
            item_sales_list.append(item_sales_dict)

            # Criar o DataFrame a partir da lista de dicionários
        item_sales_df = pd.DataFrame(item_sales_list)
        return item_sales_df

    def extract_payment_methods(self) -> pd.DataFrame:
        """Extrai as vendas de itens da tabela Bronze e transforma os dados."""
        query = f""" SELECT * FROM `colibri-413121.silver.payment_methods` WHERE extraction_date = '{self.date}' """
        print(query)
        # Executar a consulta
        query_job = self.bq.query(query)
        print(query_job)
        payment_methods_list = []
        for payment_methods in query_job:
            payment_methods_dict = dict(payment_methods)
            payment_methods_list.append(payment_methods_dict)

            # Criar o DataFrame a partir da lista de dicionários
        payment_methods_df = pd.DataFrame(payment_methods_list)
        return payment_methods_df

    def extract_revenue(self) -> pd.DataFrame:
        """Extrai as vendas de itens da tabela Bronze e transforma os dados."""
        query = f""" SELECT * FROM `colibri-413121.silver.revenue` WHERE extraction_date = '{self.date}' """
        print(query)
        # Executar a consulta
        query_job = self.bq.query(query)
        print(query_job)
        revenue_list = []
        for revenue in query_job:
            revenue_dict = dict(revenue)
            revenue_list.append(revenue_dict)

            # Criar o DataFrame a partir da lista de dicionários
        revenue_df = pd.DataFrame(revenue_list)
        return revenue_df

    def model_atendentes(self):
        df = self.extract_revenue()
        df = df[['idAtendente', 'codAtendente', 'nomeAtendente']]
        df = df.dropna(subset=['idAtendente'])
        df = df.drop_duplicates()
        return df

    def model_lojas(self):
        df = self.extract_revenue()
        df = df[['loja', 'lojaId']]
        df = df.dropna(subset=['lojaId'])
        df = df.drop_duplicates()
        return df

    def model_maquinas(self):
        df = self.extract_revenue()
        df = df[['maquinaId', 'nomeMaquina', 'caminhoMaquina']]
        df = df.dropna(subset=['maquinaId'])
        df = df.drop_duplicates()
        return df

    def model_rede(self):
        df = self.extract_revenue()
        df = df[['rede', 'redeId']]
        df = df.dropna(subset=['redeId'])
        df = df.drop_duplicates()
        return df


    def model_modoVenda(self):
        df = self.extract_revenue()
        df = df[['modoVendaId', 'modoVendaNome']]
        df = df.dropna(subset=['modoVendaId'])
        df = df.drop_duplicates()
        return df

    def model_meioPagamento(self):
        df = self.extract_payment_methods()
        df = df[['id', 'codigo', 'nome', 'redeId']]
        df = df.dropna(subset=['id'])
        df = df.drop_duplicates()
        return df

    def model_pagamentos(self):
        df = self.extract_payment_methods()
        df = df[['idMovimentoCaixa','redeId', 'valor', 'idAtendente','turnoId']]
        df = df.drop_duplicates()
        return df

    def model_material(self):
        df = self.extract_item_sales()
        df = df[['idMaterial', 'codMaterial', 'codMaterialStr', 'descricao', 'valorTotal', 'valorUnitario']]
        df = df.dropna(subset=['idMaterial'])
        df = df.drop_duplicates()
        return df

    def model_grupo(self):
        df = self.extract_item_sales()
        df = df[['codGrupo','grupoNome']]
        df = df.dropna(subset=['codGrupo'])
        df = df.drop_duplicates()
        return df

    def model_combo(self):
        df = self.extract_item_sales()
        df = df[['comboId', 'combo']]
        df = df.dropna(subset=['comboId'])
        df = df.drop_duplicates()
        return df

    def model_pontoVenda(self):
        df = self.extract_item_sales()
        df = df[['pontoVendaId', 'pontoVendaNome']]
        df = df.dropna(subset=['pontoVendaId'])
        df = df.drop_duplicates()
        return df



    def model_itensVenda(self):
        df = self.extract_item_sales()
        df = df[['idItemVenda', 'redeId', 'lojaId', 'quantidade', 'idMaterial', 'operacaoId', 'atendenteId',
                     'pontoVendaId', 'modoVendaId', 'cancelado', 'timestampLancamento', 'dtLancamento',
                     'horaLancamento', 'tipoCancelamento', 'consumidores', 'valorTotal', 'valorUnitario' ,'comboId', 'motivoCancelamento']]
        df = df.drop_duplicates()
        return df

    def model_MovimentoCaixa(self):
        df = self.extract_revenue()
        df = df[['idMovimentoCaixa', 'redeId', 'lojaId', 'hora', 'idAtendente', 'vlDesconto', 'vlAcrescimo',
                 'vlTotalReceber', 'vlTotalRecebido', 'vlServicoRecebido', 'vlConsumacaoRecebido',
                 'vlTrocoContravale', 'vlTrocoDinheiro', 'vlTrocoRepique', 'vlTaxaEntrega', 'vlEntrada',
                 'numPessoas', 'operacaoId', 'maquinaId', 'turnoId', 'consumidores', 'cancelado', 'modoVendaId',
                 'dataContabil']]
        df = df.drop_duplicates()
        return df

'''
project_id = os.environ['project_id']
date = datetime.now().strftime('%Y-%m-%d')
GoldModelData = GoldModelData(date = date, project_id= project_id)'''
