from extraction_colibri.data_extractor import SalesExtractor
from transform_data.data_converter import DataConverter
import pandas as pd
from datetime import datetime


class BronzeModelData:
    @staticmethod
    def extract_item_sales(start_date: str, end_date: str, chain: list, stores: list) -> tuple:
        item_sales_extractor = SalesExtractor(endpoint="itemvenda")
        item_sales_data = item_sales_extractor.set_params(start_date, end_date, stores, chain).extract()
        item_sales_list = []
        extraction_date = datetime.now().strftime('%Y-%m-%d')
        for item in item_sales_data:
            item_sales = item.get('data', [])
            for sale in item_sales:
                sale['extraction_date'] = extraction_date
            item_sales_list.extend(item_sales)
        item_sales = pd.json_normalize(item_sales_list)
        item_sales = DataConverter.clean_column_names(item_sales)
        item_sales = DataConverter.list_to_str(item_sales)
        return item_sales

    @staticmethod
    def extract_revenue(start_date: str, end_date: str, chain: list, stores: list) -> tuple:
        revenue_extractor = SalesExtractor(endpoint="movimentocaixa")
        revenue_data = revenue_extractor.set_params(start_date, end_date, stores, chain).extract()
        revenue_list = []
        payment_methods_list = []
        extraction_date = datetime.now().strftime('%Y-%m-%d')
        for item in revenue_data:
            revenue_sales = item.get('data', [])
            for sale in revenue_sales:
                sale['extraction_date'] = extraction_date
            revenue_list.extend(revenue_sales)
            for movimento in revenue_sales:
                payment_methods = movimento.get('meiosPagamento', [])
                for payment_method in payment_methods:
                    payment_method['extraction_date'] = extraction_date
                    payment_method['idMovimentoCaixa'] = movimento['idMovimentoCaixa']
                    payment_methods_list.append(payment_method)
        revenue = pd.json_normalize(revenue_list)
        revenue = DataConverter.clean_column_names(revenue)
        payment_methods = pd.json_normalize(payment_methods_list)
        payment_methods = DataConverter.clean_column_names(payment_methods)
        revenue = DataConverter.list_to_str(revenue)
        payment_methods = DataConverter.list_to_str(payment_methods)
        return revenue, payment_methods




