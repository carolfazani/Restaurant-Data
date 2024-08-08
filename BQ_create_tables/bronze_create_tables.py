from BQ_model_data.bronze_model_data import BronzeModelData
from config_BQ.BQ_table_creator import BigQueryTableCreator
import os


class BronzeTableCreator:
    def __init__(self, project_id, chain, stores, start_date, end_date):
        self.project_id = project_id
        self.chain = chain
        self.stores = stores
        self.start_date = start_date
        self.end_date = end_date
        self.table_creator = BigQueryTableCreator(project_id, is_bronze = True)

    def create_item_sales_table(self):
        item_sales = BronzeModelData.extract_item_sales(self.start_date, self.end_date, self.chain, self.stores)
        self.table_creator.create_table(item_sales, table_name='item_sales', dataset_id='bronze')

    def create_revenue_table(self):
        revenue, payment_methods = BronzeModelData.extract_revenue(self.start_date, self.end_date, self.chain, self.stores)
        self.table_creator.create_table(revenue, table_name='revenue', dataset_id='bronze')
        self.table_creator.create_table(payment_methods, table_name='payment_methods', dataset_id='bronze')

    def run(self):
        self.create_item_sales_table()
        self.create_revenue_table()


if __name__ == "__main__":
    stores = os.environ['MC_LOJA']
    chain = os.environ['MC_REDE']
    project_id = os.environ['PROJECT_ID']
    start_date = '2023-06-01'
    end_date = '2024-06-12'
    pipeline = BronzeTableCreator(project_id, chain, stores, start_date, end_date)
    pipeline.run()
