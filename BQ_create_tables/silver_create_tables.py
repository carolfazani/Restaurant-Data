from BQ_model_data.silver_model_data import SilverModelData
from config_BQ.BQ_table_creator import BigQueryTableCreator
import os
from datetime import datetime

project_id = os.environ['project_id']
SalesModelData = SilverModelData
date = datetime.now().strftime('%Y-%m-%d')
print(SalesModelData.extract_item_sales(date= date))

def create_item_sales_table():
    item_sales = SilverModelData.extract_item_sales(date)
    table_creator = BigQueryTableCreator(project_id)
    print(table_creator)
    table_creator.create_table(item_sales, table_name='item_sales', dataset_id='silver')

create_item_sales_table()


