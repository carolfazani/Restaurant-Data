from BQ_data_loading.bronze_insert_data import BronzeDataPipeline
from BQ_data_loading.silver_insert_data import SilverDataPipeline
from BQ_data_loading.gold_insert_data import GoldDataPipeline

import os
from datetime import datetime

def run_bronze_pipeline():
    stores = os.environ['MC_LOJA']
    chain = os.environ['MC_REDE']
    project_id = os.environ['project_id']
    start_date = '2024-01-01'
    end_date = '2024-06-08'

    pipeline = BronzeDataPipeline(project_id, chain, stores, start_date, end_date)
    pipeline.run()

def run_silver_pipeline():
    project_id = os.environ['project_id']
    date = datetime.now().strftime('%Y-%m-%d')

    pipeline = SilverDataPipeline(project_id, date)
    pipeline.run()

def run_gold_pipeline():
    project_id = os.environ['project_id']
    date = datetime.now().strftime('%Y-%m-%d')

    pipeline = GoldDataPipeline(project_id, date)
    pipeline.run()

if __name__ == "__main__":
    #run_bronze_pipeline()
    #run_silver_pipeline()
    run_gold_pipeline()
