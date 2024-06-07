from BQ_model_data.silver_model_data import SilverModelData
from config_BQ.columnTypes import generate_column_types
import os
from database.bigQuery import BigQueryPython

from datetime import  datetime


project_id = os.environ['project_id']


date = datetime.now().strftime('%Y-%m-%d')

def insert_item_sales():

    bq = BigQueryPython(project=project_id)
    item_sales = SilverModelData.extract_item_sales(date)
    column_types =  generate_column_types(item_sales)
    bq.load_dataframe_to_bigquery(dataframe= item_sales, dataset_id= 'silver', table_name= 'item_sales', column_types= column_types, load_mode='WRITE_TRUNCATE')

insert_item_sales()