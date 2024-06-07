from BQ_model_data.bronze_model_data import BronzeModelData
from config_BQ.columnTypes import generate_column_types_bronze
import os
from database.bigQuery import BigQueryPython


class BronzeDataPipeline:
    def __init__(self, project_id, chain, stores, start_date, end_date):
        self.project_id = project_id
        self.chain = chain
        self.stores = stores
        self.start_date = start_date
        self.end_date = end_date
        self.bq = BigQueryPython(project=project_id)

    def insert_item_sales(self):
        item_sales = BronzeModelData.extract_item_sales(self.start_date, self.end_date, self.chain, self.stores)
        item_sales = item_sales.astype(str)
        column_types = generate_column_types_bronze(item_sales)
        self.bq.load_dataframe_to_bigquery(dataframe=item_sales, dataset_id='bronze', table_name='item_sales', column_types=column_types, load_mode='WRITE_TRUNCATE')

    def insert_revenue(self):
        revenue, payment_methods = BronzeModelData.extract_revenue(self.start_date, self.end_date, self.chain, self.stores)
        revenue = revenue.astype(str)
        payment_methods = payment_methods.astype(str)

        revenue_column_types = generate_column_types_bronze(revenue)
        self.bq.load_dataframe_to_bigquery(dataframe=revenue, dataset_id='bronze', table_name='revenue', column_types=revenue_column_types, load_mode='WRITE_TRUNCATE')

        payment_methods_column_types = generate_column_types_bronze(payment_methods)
        self.bq.load_dataframe_to_bigquery(dataframe=payment_methods, dataset_id='bronze', table_name='payment_methods', column_types=payment_methods_column_types, load_mode='WRITE_TRUNCATE')

    def run(self):
        self.insert_item_sales()
        self.insert_revenue()

        # Exemplo de uso

if __name__ == "__main__":
    stores = os.environ['MC_LOJA']
    chain = os.environ['MC_REDE']
    project_id = os.environ['project_id']
    start_date = '2024-01-01'
    end_date = '2024-01-31'

    pipeline = BronzeDataPipeline(project_id, chain, stores, start_date, end_date)
    pipeline.run()

