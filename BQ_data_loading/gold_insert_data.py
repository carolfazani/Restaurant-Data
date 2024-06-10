from BQ_model_data.gold_model_data import GoldModelData
from config_BQ.columnTypes import generate_column_types
import os
from database.bigQuery import BigQueryPython

from datetime import  datetime

class GoldDataPipeline(GoldModelData):
    def __init__(self, project_id, date):
        super().__init__(project_id=project_id, date=date)
        self.bq = BigQueryPython(project=project_id)

    def insert_atententes(self):
        dataset_id = 'gold'
        table_id = 'dim_atendentes'
        df = self.model_atendentes()
        column_types = generate_column_types(df)
        self.bq.load_dataframe_to_bigquery(dataframe=df, dataset_id=dataset_id, table_name=table_id,
                                      column_types=column_types, load_mode='WRITE_TRUNCATE')

    def insert_lojas(self):
        dataset_id = 'gold'
        table_id = 'dim_lojas'
        df = self.model_lojas()
        column_types = generate_column_types(df)
        self.bq.load_dataframe_to_bigquery(dataframe=df, dataset_id=dataset_id, table_name=table_id,
                                      column_types=column_types, load_mode='WRITE_TRUNCATE')

    def insert_maquinas(self):
        dataset_id = 'gold'
        table_id = 'dim_maquinas'
        df = self.model_maquinas()
        column_types = generate_column_types(df)
        self.bq.load_dataframe_to_bigquery(dataframe=df, dataset_id=dataset_id, table_name=table_id,
                                      column_types=column_types, load_mode='WRITE_TRUNCATE')

    def insert_pontoVenda(self):
        dataset_id = 'gold'
        table_id = 'dim_pontosVenda'
        df = self.model_pontoVenda()
        column_types = generate_column_types(df)
        self.bq.load_dataframe_to_bigquery(dataframe=df, dataset_id=dataset_id, table_name=table_id,
                                      column_types=column_types, load_mode='WRITE_TRUNCATE')

    def insert_material(self):
        dataset_id = 'gold'
        table_id = 'dim_materiais'
        df = self.model_material()
        column_types = generate_column_types(df)
        self.bq.load_dataframe_to_bigquery(dataframe=df, dataset_id=dataset_id, table_name=table_id,
                                      column_types=column_types, load_mode='WRITE_TRUNCATE')

    def insert_meioPagamento(self):
        dataset_id = 'gold'
        table_id = 'dim_meiosPagamento'
        df = self.model_meioPagamento()
        column_types = generate_column_types(df)
        self.bq.load_dataframe_to_bigquery(dataframe=df, dataset_id=dataset_id, table_name=table_id,
                                      column_types=column_types, load_mode='WRITE_TRUNCATE')

    def insert_modoVenda(self):
        dataset_id = 'gold'
        table_id = 'dim_modosVenda'
        df = self.model_modoVenda()
        column_types = generate_column_types(df)
        self.bq.load_dataframe_to_bigquery(dataframe=df, dataset_id=dataset_id, table_name=table_id,
                                      column_types=column_types, load_mode='WRITE_TRUNCATE')


    def insert_rede(self):
        dataset_id = 'gold'
        table_id = 'dim_redes'
        df = self.model_rede()
        column_types = generate_column_types(df)
        self.bq.load_dataframe_to_bigquery(dataframe=df, dataset_id=dataset_id, table_name=table_id,
                                      column_types=column_types, load_mode='WRITE_TRUNCATE')

    def insert_movimentoCaixa(self):
        dataset_id = 'gold'
        table_id = 'ft_movimentoCaixas'
        df = self.model_MovimentoCaixa()
        column_types = generate_column_types(df)
        self.bq.load_dataframe_to_bigquery(dataframe=df, dataset_id=dataset_id, table_name=table_id,
                                      column_types=column_types, load_mode='WRITE_TRUNCATE')

    def insert_itensVenda(self):
        dataset_id = 'gold'
        table_id = 'ft_itensVenda'
        df = self.model_itensVenda()
        column_types = generate_column_types(df)
        self.bq.load_dataframe_to_bigquery(dataframe=df, dataset_id=dataset_id, table_name=table_id,
                                      column_types=column_types, load_mode='WRITE_TRUNCATE')

    def insert_pagamentos(self):
        dataset_id = 'gold'
        table_id = 'ft_pagamentos'
        df = self.model_pagamentos()
        column_types = generate_column_types(df)
        self.bq.load_dataframe_to_bigquery(dataframe=df, dataset_id=dataset_id, table_name=table_id,
                                      column_types=column_types, load_mode='WRITE_TRUNCATE')

    def run(self):
        self.insert_atententes()
        self.insert_lojas()
        self.insert_maquinas()
        self.insert_pontoVenda()
        self.insert_material()
        self.insert_meioPagamento()
        self.insert_modoVenda()
        self.insert_rede()
        self.insert_movimentoCaixa()
        self.insert_itensVenda()
        self.insert_pagamentos()



if __name__ == "__main__":
    project_id = os.environ['project_id']
    date = datetime.now().strftime('%Y-%m-%d')
    pipeline = GoldDataPipeline(project_id, date)
    pipeline.run()