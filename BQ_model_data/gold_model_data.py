from database.bigQuery import BigQueryPython
import pandas as pd



class GoldModelData:
    def __init__(self, project_id: str, date: str):
        self.project_id = project_id
        self.bq = BigQueryPython(project=project_id)
        self.date = date


    def extract_item_sales(self) -> pd.DataFrame:
        """Extrai as vendas de itens da tabela Bronze e transforma os dados."""
        query = f""" SELECT * FROM `team-analytics-datalake.silver.item_sales` WHERE extraction_date = '{self.date}' """
        query_job = self.bq.query(query)
        print(query_job)
        item_sales_list = []
        for item_sales in query_job:
            item_sales_dict = dict(item_sales)
            item_sales_list.append(item_sales_dict)
        item_sales_df = pd.DataFrame(item_sales_list)
        return item_sales_df

    def extract_payment_methods(self) -> pd.DataFrame:
        query = f""" SELECT * FROM `team-analytics-datalake.silver.payment_methods` WHERE extraction_date = '{self.date}'"""

        query_job = self.bq.query(query)
        payment_methods_list = []
        for payment_methods in query_job:
            payment_methods_dict = dict(payment_methods)
            payment_methods_list.append(payment_methods_dict)

        payment_methods_df = pd.DataFrame(payment_methods_list)
        return payment_methods_df

    def extract_revenue(self) -> pd.DataFrame:
        """Extrai as vendas de itens da tabela Bronze e transforma os dados."""
        query = f""" SELECT * FROM `team-analytics-datalake.silver.revenue` WHERE extraction_date = '{self.date}' """
        query_job = self.bq.query(query)
        revenue_list = []
        for revenue in query_job:
            revenue_dict = dict(revenue)
            revenue_list.append(revenue_dict)
        revenue_df = pd.DataFrame(revenue_list)
        return revenue_df

    def model_atendentes(self):
        query = f""" SELECT DISTINCT idAtendente, codAtendente, nomeAtendente FROM `team-analytics-datalake.silver.revenue` """
        query_job = self.bq.query(query)
        data_list = []
        for data in query_job:
            data_dict = dict(data)
            data_list.append(data_dict)
        data_df = pd.DataFrame(data_list)
        return data_df



    def model_lojas(self):
        query = f""" SELECT DISTINCT loja, lojaId FROM `team-analytics-datalake.silver.revenue` """
        query_job = self.bq.query(query)
        data_list = []
        for data in query_job:
            data_dict = dict(data)
            data_list.append(data_dict)
        data_df = pd.DataFrame(data_list)
        return data_df

    def model_maquinas(self):
        query = f""" SELECT DISTINCT maquinaId, nomeMaquina, caminhoMaquina FROM `team-analytics-datalake.silver.revenue`"""
        query_job = self.bq.query(query)
        data_list = []
        for data in query_job:
            data_dict = dict(data)
            data_list.append(data_dict)
        data_df = pd.DataFrame(data_list)
        return data_df

    def model_rede(self):
        query = f""" SELECT DISTINCT rede, redeId FROM `team-analytics-datalake.silver.revenue`"""
        query_job = self.bq.query(query)
        data_list = []
        for data in query_job:
            data_dict = dict(data)
            data_list.append(data_dict)
        data_df = pd.DataFrame(data_list)
        return data_df


    def model_modoVenda(self):
        query = f""" SELECT DISTINCT modoVendaId, modoVendaNome FROM `team-analytics-datalake.silver.revenue`"""
        query_job = self.bq.query(query)
        data_list = []
        for data in query_job:
            data_dict = dict(data)
            data_list.append(data_dict)
        data_df = pd.DataFrame(data_list)
        return data_df

    def model_meioPagamento(self):
        query = f""" SELECT DISTINCT id, codigo, nome, redeId FROM `team-analytics-datalake.silver.payment_methods`"""
        query_job = self.bq.query(query)
        data_list = []
        for data in query_job:
            data_dict = dict(data)
            data_list.append(data_dict)
        data_df = pd.DataFrame(data_list)
        return data_df


    def model_material(self):
        query = f""" SELECT DISTINCT codMaterial, descricao FROM `team-analytics-datalake.silver.item_sales`"""
        query_job = self.bq.query(query)
        data_list = []
        for data in query_job:
            data_dict = dict(data)
            data_list.append(data_dict)
        data_df = pd.DataFrame(data_list)
        return data_df

    def model_grupo(self):
        query = f""" SELECT DISTINCT codGrupo,grupoNome FROM `team-analytics-datalake.silver.item_sales`"""
        query_job = self.bq.query(query)
        data_list = []
        for data in query_job:
            data_dict = dict(data)
            data_list.append(data_dict)
        data_df = pd.DataFrame(data_list)
        return data_df


    def model_combo(self):
        query = f""" SELECT DISTINCT comboId, combo FROM `team-analytics-datalake.silver.item_sales`"""
        query_job = self.bq.query(query)
        data_list = []
        for data in query_job:
            data_dict = dict(data)
            data_list.append(data_dict)
        data_df = pd.DataFrame(data_list)
        return data_df

    def model_pontoVenda(self):
        query = f""" SELECT DISTINCT pontoVendaId, pontoVendaNome FROM `team-analytics-datalake.silver.item_sales`"""
        query_job = self.bq.query(query)
        data_list = []
        for data in query_job:
            data_dict = dict(data)
            data_list.append(data_dict)
        data_df = pd.DataFrame(data_list)
        return data_df


    def model_itensVenda(self):
        query = f""" SELECT idItemVenda, redeId, lojaId, quantidade, codMaterial, operacaoId, atendenteId, pontoVendaId, modoVendaId, cancelado, timestampLancamento, dtLancamento,
                     horaLancamento, tipoCancelamento, consumidores, valorTotal, valorUnitario ,comboId, motivoCancelamento, codGrupo FROM `team-analytics-datalake.silver.item_sales`  WHERE extraction_date = '{self.date}'"""
        query_job = self.bq.query(query)
        data_list = []
        for data in query_job:
            data_dict = dict(data)
            data_list.append(data_dict)
        data_df = pd.DataFrame(data_list)
        data_df['timestampLancamento'] = pd.to_datetime(data_df['timestampLancamento'], format='%Y-%m-%d')
        data_df['dtLancamento'] = pd.to_datetime(data_df['dtLancamento'], format='%Y-%m-%d')
        return data_df

    def model_MovimentoCaixa(self):
        query = f""" SELECT idMovimentoCaixa, redeId, lojaId, hora, idAtendente, vlDesconto, vlAcrescimo,
                 vlTotalReceber, vlTotalRecebido, vlServicoRecebido, vlConsumacaoRecebido,
                 vlTrocoContravale, vlTrocoDinheiro, vlTrocoRepique, vlTaxaEntrega, vlEntrada,
                 numPessoas, operacaoId, maquinaId, turnoId, consumidores, cancelado, modoVendaId,
                 dataContabil FROM `team-analytics-datalake.silver.revenue`  WHERE extraction_date = '{self.date}'"""
        query_job = self.bq.query(query)
        data_list = []
        for data in query_job:
            data_dict = dict(data)
            data_list.append(data_dict)
        data_df = pd.DataFrame(data_list)
        data_df['dataContabil'] = pd.to_datetime(data_df['dataContabil'])
        print(data_df)
        return data_df

    def model_pagamentos(self):
        query = f""" SELECT idMovimentoCaixa,redeId, valor, idAtendente,turnoId, id, codigo FROM `team-analytics-datalake.silver.payment_methods`  WHERE extraction_date = '{self.date}'"""
        query_job = self.bq.query(query)
        data_list = []
        for data in query_job:
            data_dict = dict(data)
            data_list.append(data_dict)
        data_df = pd.DataFrame(data_list)
        return data_df

'''
project_id = os.environ['project_id']
date = datetime.now().strftime('%Y-%m-%d')
GoldModelData = GoldModelData(date = date, project_id= project_id)'''
