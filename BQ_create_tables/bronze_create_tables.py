from BQ_model_data.bronze_model_data import SalesModelData
from config_BQ.BQ_table_creator import BigQueryTableCreator
import os

stores = os.environ['MC_LOJA']
chain = os.environ['MC_REDE']
project_id = os.environ['project_id']
start_date = '2024-01-01'
end_date = '2024-01-31'

def create_item_sales_table():
    item_sales = SalesModelData.extract_item_sales(start_date, end_date, chain, stores)
    table_creator = BigQueryTableCreator(project_id)
    table_creator.create_table(item_sales, table_name='item_sales', dataset_id='bronze')


def create_revenue_table():
    revenue, payment_methods = SalesModelData.extract_revenue(start_date, end_date, chain, stores)
    table_creator = BigQueryTableCreator(project_id)
    table_creator.create_table(revenue, table_name='revenue', dataset_id='bronze')
    table_creator.create_table(payment_methods, table_name='payment_methods', dataset_id='bronze')


create_item_sales_table()
create_revenue_table()