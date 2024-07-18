from BQ_model_data.silver_model_data import SilverModelData
from config_BQ.columnTypes import generate_column_types
from database.bigQuery import BigQueryPython




class SilverDataPipeline(SilverModelData):
    def __init__(self, project_id, date):
        super().__init__(project_id=project_id, date=date)
        self.bq = BigQueryPython(project=project_id)

    def insert_item_sales(self):
        item_sales = self.extract_item_sales()
        column_types = generate_column_types(item_sales)
        self.bq.load_dataframe_to_bigquery(dataframe=item_sales, dataset_id='silver', table_name='item_sales',
                                      column_types=column_types, load_mode='WRITE_APPEND') #td append

    def insert_revenue(self):
        revenue = self.extract_revenue()
        column_types = generate_column_types(revenue)
        self.bq.load_dataframe_to_bigquery(dataframe=revenue, dataset_id='silver', table_name='revenue',
                                      column_types=column_types, load_mode='WRITE_APPEND')
    def insert_payment_methods(self):
        payment_methods = self.extract_payment_methods()
        column_types = generate_column_types(payment_methods)
        self.bq.load_dataframe_to_bigquery(dataframe=payment_methods, dataset_id='silver', table_name='payment_methods',
                                      column_types=column_types, load_mode='WRITE_APPEND')

    def run(self):
        self.insert_item_sales()
        self.insert_revenue()
        self.insert_payment_methods()



