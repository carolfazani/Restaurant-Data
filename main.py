from mysql_data_loading.colibri_data_loading.revenue_mysql_loader import insert_movimento_caixa
from mysql_data_loading.colibri_data_loading.itens_sales_mysql_loader import insert_itens_venda
from mysql_data_loading.sheets_data_loading.sheets_mysql_loader import insert_recebimentos

def main():
    insert_itens_venda()
    insert_movimento_caixa()
    insert_recebimentos()

if __name__ == '__main__':
    main()