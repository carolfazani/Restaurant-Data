from BQ_model_data.bronze_model_data import BronzeModelData
from transform_data.data_converter import *
import os
from database.bigQuery import BigQueryPython
from datetime import datetime
import pandas as pd


class SilverModelData:
    # Configurar o cliente BigQuery
    project_id = os.environ['project_id']
    bq = BigQueryPython(project=project_id)
    @staticmethod
    def extract_item_sales(date: str) -> pd.DataFrame:

        """Extrai as vendas de itens da tabela Bronze e transforma os dados."""
        query = f""" SELECT * FROM `colibri-413121.bronze.item_sales` WHERE extraction_date = '{date}' """
        # Executar a consulta
        query_job = SilverModelData.bq.query(query)
        sale_list = []
        for sale in query_job:
            sale_dict = dict(sale)
            sale_dict['idItemVenda'] = DataConverter.to_str(sale_dict['idItemVenda'])
            sale_dict['redeId'] = DataConverter.to_str(sale_dict['redeId'])
            sale_dict['lojaId'] = DataConverter.to_str(sale_dict['lojaId'])
            sale_dict['quantidade'] = DataConverter.to_float(sale_dict['quantidade'])
            sale_dict['idMaterial'] = DataConverter.to_str(sale_dict['idMaterial'])
            sale_dict['codMaterial'] = DataConverter.to_str(sale_dict['codMaterial'])
            sale_dict['descricao'] = DataConverter.to_str(sale_dict['descricao'])
            sale_dict['valorTotal'] = DataConverter.to_float(sale_dict['valorTotal'])
            sale_dict['valorUnitario'] = DataConverter.to_float(sale_dict['valorUnitario'])
            sale_dict['codGrupo'] = DataConverter.to_str(sale_dict['codGrupo'])
            sale_dict['grupoNome'] = DataConverter.to_str(sale_dict['grupoNome'])
            sale_dict['comboId'] = DataConverter.to_str(sale_dict['comboId'])
            sale_dict['combo'] = DataConverter.to_str(sale_dict['combo'])
            sale_dict['operacaoId'] = DataConverter.to_str(sale_dict['operacaoId'])
            sale_dict['atendenteId'] = DataConverter.to_str(sale_dict['atendenteId'])
            sale_dict['atendenteNome'] = DataConverter.to_str(sale_dict['atendenteNome'])
            sale_dict['motivoCancelamento'] = DataConverter.to_str(sale_dict['motivoCancelamento'])
            sale_dict['pontoVendaId'] = DataConverter.to_str(sale_dict['pontoVendaId'])
            sale_dict['pontoVendaNome'] = DataConverter.to_str(sale_dict['pontoVendaNome'])
            sale_dict['modoVendaId'] = DataConverter.to_str(sale_dict['modoVendaId'])
            sale_dict['modoVendaNome'] = DataConverter.to_str(sale_dict['modoVendaNome'])
            sale_dict['cancelado'] = DataConverter.to_boolean(sale_dict['cancelado'])
            sale_dict['timestampLancamento'] = DataConverter.to_datetime_with_seconds(sale_dict['timestampLancamento'])
            sale_dict['dtLancamento'] = DataConverter.to_datetime(sale_dict['dtLancamento'])
            sale_dict['horaLancamento'] = DataConverter.to_seconds(sale_dict['horaLancamento'])
            sale_dict['tipoCancelamento'] = DataConverter.to_str(sale_dict['tipoCancelamento'])
            sale_dict['extraction_date'] = DataConverter.to_str(sale_dict['extraction_date'])
            sale_list.append(sale_dict)
        silver_dataframe = pd.DataFrame(sale_list)
        return silver_dataframe

