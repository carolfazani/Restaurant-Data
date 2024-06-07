from BQ_model_data.silver_model_data import SilverModelData
from config_BQ.columnTypes import generate_column_types
import os
from database.bigQuery import BigQueryPython

from datetime import  datetime



class SilverDataPipeline(SilverModelData):
    def __init__(self, project_id, date):
        super().__init__(project_id=project_id, date=date)
        self.bq = BigQueryPython(project=project_id)

    def insert_item_sales(self):
        bq = BigQueryPython(project=project_id)
        item_sales = self.extract_item_sales()
        column_types = generate_column_types(item_sales)
        bq.load_dataframe_to_bigquery(dataframe=item_sales, dataset_id='silver', table_name='item_sales',
                                      column_types=column_types, load_mode='WRITE_TRUNCATE')

    def insert_revenue(self):
        bq = BigQueryPython(project=project_id)
        revenue = self.extract_revenue()
        column_types = generate_column_types(revenue)
        bq.load_dataframe_to_bigquery(dataframe=revenue, dataset_id='silver', table_name='revenue',
                                      column_types=column_types, load_mode='WRITE_TRUNCATE')
    def insert_payment_methods(self):
        bq = BigQueryPython(project=project_id)
        payment_methods = self.extract_payment_methods()
        column_types = generate_column_types(payment_methods)
        bq.load_dataframe_to_bigquery(dataframe=payment_methods, dataset_id='silver', table_name='payment_methods',
                                      column_types=column_types, load_mode='WRITE_TRUNCATE')

    def run(self):
        self.insert_item_sales()
        self.insert_revenue()
        self.insert_payment_methods()

        # Exemplo de uso

if __name__ == "__main__":
    project_id = os.environ['project_id']
    date = datetime.now().strftime('%Y-%m-%d')
    pipeline = SilverDataPipeline(project_id, date)
    pipeline.run()

