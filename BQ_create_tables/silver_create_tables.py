from BQ_model_data.silver_model_data import SilverModelData
from config_BQ.BQ_table_creator import BigQueryTableCreator
import os
from datetime import datetime

class SilverTableCreator(SilverModelData):
    def __init__(self, project_id, date):
        super().__init__(project_id=project_id, date=date)
        self.table_creator = BigQueryTableCreator(project_id)

    def create_item_sales_table(self):
        item_sales = self.extract_item_sales()
        self.table_creator.create_table(item_sales, table_name='item_sales', dataset_id='silver')


    def create_revenue_table(self):
        revenue = self.extract_revenue()
        self.table_creator.create_table(revenue, table_name='revenue', dataset_id='silver')

    def create_payment_methods_table(self):
        payment_methods = self.extract_payment_methods()
        self.table_creator.create_table(payment_methods, table_name='payment_methods', dataset_id='silver')

    def run(self):
        self.create_item_sales_table()
        self.create_revenue_table()
        self.create_payment_methods_table()




# Example usage
if __name__ == "__main__":
    project_id = os.environ['project_id']
    date = datetime.now().strftime('%Y-%m-%d')
    #date = '2024-06-10'
    pipeline = SilverTableCreator(project_id, date)
    pipeline.run()


