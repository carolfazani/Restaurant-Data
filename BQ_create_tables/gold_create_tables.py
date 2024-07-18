from BQ_model_data.gold_model_data import GoldModelData
from config_BQ.BQ_table_creator import BigQueryTableCreator
import os
from datetime import datetime

class GoldTableCreator(GoldModelData):
    def __init__(self, project_id, date):
        super().__init__(project_id=project_id, date=date)
        self.table_creator = BigQueryTableCreator(project_id)

    def insert_atententes(self):
        dataset_id = 'gold'
        table_id = 'dim_atendentes'
        df = self.model_atendentes()
        self.table_creator.create_table(df, table_name=table_id, dataset_id=dataset_id, not_null_columns=['idAtendente'])

    def insert_lojas(self):
        dataset_id = 'gold'
        table_id = 'dim_lojas'
        df = self.model_lojas()
        self.table_creator.create_table(df, table_name=table_id, dataset_id=dataset_id, not_null_columns=['lojaId'])

    def insert_maquinas(self):
        dataset_id = 'gold'
        table_id = 'dim_maquinas'
        df = self.model_maquinas()
        self.table_creator.create_table(df, table_name=table_id, dataset_id=dataset_id, not_null_columns=['maquinaId'])

    def insert_pontoVenda(self):
        dataset_id = 'gold'
        table_id = 'dim_pontosVenda'
        df = self.model_pontoVenda()
        self.table_creator.create_table(df, table_name=table_id, dataset_id=dataset_id, not_null_columns=['pontoVendaId'])

    def insert_material(self):
        dataset_id = 'gold'
        table_id = 'dim_materiais'
        df = self.model_material()
        self.table_creator.create_table(df, table_name=table_id, dataset_id=dataset_id, not_null_columns=['codMaterial'])

    def insert_meioPagamento(self):
        dataset_id = 'gold'
        table_id = 'dim_meiosPagamento'
        df = self.model_meioPagamento()
        self.table_creator.create_table(df, table_name=table_id, dataset_id=dataset_id, not_null_columns=['id'])

    def insert_modoVenda(self):
        dataset_id = 'gold'
        table_id = 'dim_modosVenda'
        df = self.model_modoVenda()
        self.table_creator.create_table(df, table_name=table_id, dataset_id=dataset_id, not_null_columns=['modoVendaId'])


    def insert_rede(self):
        dataset_id = 'gold'
        table_id = 'dim_redes'
        df = self.model_rede()
        self.table_creator.create_table(df, table_name=table_id, dataset_id=dataset_id, not_null_columns=['redeId'])

    def insert_movimentoCaixa(self):
        dataset_id = 'gold'
        table_id = 'ft_movimentoCaixas'
        df = self.model_MovimentoCaixa()
        self.table_creator.create_table(df, table_name=table_id, dataset_id=dataset_id)

    def insert_itensVenda(self):
        dataset_id = 'gold'
        table_id = 'ft_itensVenda'
        df = self.model_itensVenda()
        self.table_creator.create_table(df, table_name=table_id, dataset_id=dataset_id)

    def insert_pagamentos(self):
        dataset_id = 'gold'
        table_id = 'ft_pagamentos'
        df = self.model_pagamentos()
        self.table_creator.create_table(df, table_name=table_id, dataset_id=dataset_id)

    def run(self):
        self.insert_lojas()
        self.insert_pagamentos()
        self.insert_meioPagamento()
        self.insert_atententes()
        self.insert_maquinas()
        self.insert_pontoVenda()
        
        self.insert_modoVenda()
        self.insert_rede()
        self.insert_movimentoCaixa()
        self.insert_itensVenda()
        self.insert_material()




if __name__ == "__main__":
    project_id = os.environ['PROJECT_ID']
    date = datetime.now().strftime('%Y-%m-%d')
    pipeline = GoldTableCreator(project_id, date)
    pipeline.run()

