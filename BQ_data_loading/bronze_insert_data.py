from BQ_model_data.bronze_model_data import BronzeModelData
from config_BQ.columnTypes import generate_column_types_bronze
import os
from database.bigQuery import BigQueryPython



stores = os.environ['MC_LOJA']
chain = os.environ['MC_REDE']
project_id = os.environ['project_id']
start_date = '2024-01-01'
end_date = '2024-01-31'
bq =  BigQueryPython(project=project_id)


def insert_item_sales():
    revenue, payment_methods = BronzeModelData.extract_revenue(start_date, end_date, chain, stores)
    revenue = revenue.astype(str)
    payment_methods = payment_methods.astype(str)
    column_types =  generate_column_types_bronze(revenue)
    bq.load_dataframe_to_bigquery(dataframe= revenue, dataset_id= 'bronze', table_name= 'revenue', column_types= column_types, load_mode='WRITE_TRUNCATE')
    column_types =  generate_column_types_bronze(payment_methods)
    bq.load_dataframe_to_bigquery(dataframe= payment_methods, dataset_id= 'bronze', table_name= 'payment_methods', column_types= column_types, load_mode='WRITE_TRUNCATE')

def insert_item_sales():
    item_sales = BronzeModelData.extract_item_sales(start_date, end_date, chain, stores)
    item_sales = item_sales.astype(str)
    column_types =  generate_column_types_bronze(item_sales)
    bq.load_dataframe_to_bigquery(dataframe= item_sales, dataset_id= 'bronze', table_name= 'item_sales', column_types= column_types, load_mode='WRITE_TRUNCATE')

