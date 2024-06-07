from config_BQ.model_data import *
from config_BQ.columnTypes import generate_column_types
from database.bigQuery import BigQueryPython

project = 'Colibri'
bq =  BigQueryPython(project=project)
def insert_atendentes():
    dados = model_atendentes()
    table_name = 'dim_Atendente'
    dataset_id = 'dim_tables'
    column_types =  generate_column_types(dados)

    bq.load_dataframe_to_bigquery(dataframe= dados, dataset_id= dataset_id, table_name= table_name, column_types= column_types, load_mode='WRITE_TRUNCATE')




def insert_data(dados,table_name, dataset_id):
    column_types =  generate_column_types(dados)
    bq.load_dataframe_to_bigquery(dataframe= dados, dataset_id= dataset_id, table_name= table_name, column_types= column_types, load_mode='WRITE_TRUNCATE')

insert_data(dados=model_atendentes(), table_name='dim_Atendente', dataset_id = 'dim_tables')

'''print('model_pontoVenda():',model_pontoVenda())
print('model_material():',model_material())
print('model_meioPagamento():',model_meioPagamento())
print('model_modoVenda():',model_modoVenda())
print('model_turno():',model_turno())
print('model_operacao():',model_operacao())
print('model_rede():',model_rede())
print('model_maquinas():',model_maquinas())
print('model_lojas():',model_lojas())
print('model_atendentes():',model_atendentes())'''