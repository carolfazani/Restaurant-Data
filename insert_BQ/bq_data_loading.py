from database.bigQuery import BigQueryPython
from config_BQ.columnTypes import generate_column_types
from config_BQ.model_data import *

#puxar no main

project = 'colibri-413121'
bq = BigQueryPython(project=project)

def atententes_loading():
    dataset_id = 'dim_tables'
    table_id = 'dim_Atendente'
    df = model_atendentes()
    column_types = generate_column_types(df)
    bq.load_dataframe_to_bigquery(dataframe=df, dataset_id=dataset_id, table_name=table_id,
                                  column_types=column_types, load_mode='WRITE_TRUNCATE')

def lojas_loading():
    dataset_id = 'dim_tables'
    table_id = 'dim_Loja'
    df = model_lojas()
    column_types = generate_column_types(df)
    bq.load_dataframe_to_bigquery(dataframe=df, dataset_id=dataset_id, table_name=table_id,
                                  column_types=column_types, load_mode='WRITE_TRUNCATE')

def maquinas_loading():
    dataset_id = 'dim_tables'
    table_id = 'dim_Maquina'
    df = model_maquinas()
    column_types = generate_column_types(df)
    bq.load_dataframe_to_bigquery(dataframe=df, dataset_id=dataset_id, table_name=table_id,
                                  column_types=column_types, load_mode='WRITE_TRUNCATE')

def pontoVenda_loading():
    dataset_id = 'dim_tables'
    table_id = 'dim_PontoVenda'
    df = model_pontoVenda()
    column_types = generate_column_types(df)
    bq.load_dataframe_to_bigquery(dataframe=df, dataset_id=dataset_id, table_name=table_id,
                                  column_types=column_types, load_mode='WRITE_TRUNCATE')

def material_loading():
    dataset_id = 'dim_tables'
    table_id = 'dim_Material'
    df = model_material()
    column_types = generate_column_types(df)
    bq.load_dataframe_to_bigquery(dataframe=df, dataset_id=dataset_id, table_name=table_id,
                                  column_types=column_types, load_mode='WRITE_TRUNCATE')

def meioPagamento_loading():
    dataset_id = 'dim_tables'
    table_id = 'dim_MeioPagamento'
    df = model_meioPagamento()
    column_types = generate_column_types(df)
    bq.load_dataframe_to_bigquery(dataframe=df, dataset_id=dataset_id, table_name=table_id,
                                  column_types=column_types, load_mode='WRITE_TRUNCATE')

def modoVenda_loading():
    dataset_id = 'dim_tables'
    table_id = 'dim_ModoVenda'
    df = model_modoVenda()
    column_types = generate_column_types(df)
    bq.load_dataframe_to_bigquery(dataframe=df, dataset_id=dataset_id, table_name=table_id,
                                  column_types=column_types, load_mode='WRITE_TRUNCATE')

def turno_loading():
    dataset_id = 'dim_tables'
    table_id = 'dim_Turno'
    df = model_turno()
    column_types = generate_column_types(df)
    bq.load_dataframe_to_bigquery(dataframe=df, dataset_id=dataset_id, table_name=table_id,
                                  column_types=column_types, load_mode='WRITE_TRUNCATE')

def operacao_loading():
    dataset_id = 'dim_tables'
    table_id = 'dim_Operacao'
    df = model_operacao()
    column_types = generate_column_types(df)
    bq.load_dataframe_to_bigquery(dataframe=df, dataset_id=dataset_id, table_name=table_id,
                                  column_types=column_types, load_mode='WRITE_TRUNCATE')

def rede_loading():
    dataset_id = 'dim_tables'
    table_id = 'dim_Rede'
    df = model_rede()
    column_types = generate_column_types(df)
    bq.load_dataframe_to_bigquery(dataframe=df, dataset_id=dataset_id, table_name=table_id,
                                  column_types=column_types, load_mode='WRITE_TRUNCATE')

def movimentoCaixa_loading():
    dataset_id = 'ft_tables'
    table_id = 'ft_MovimentoCaixa'
    df = model_MovimentoCaixa()
    column_types = generate_column_types(df)
    bq.load_dataframe_to_bigquery(dataframe=df, dataset_id=dataset_id, table_name=table_id,
                                  column_types=column_types, load_mode='WRITE_TRUNCATE')

def itensVenda_loading():
    dataset_id = 'ft_tables'
    table_id = 'ft_ItensVenda'
    df = model_ItensVenda()
    column_types = generate_column_types(df)
    bq.load_dataframe_to_bigquery(dataframe=df, dataset_id=dataset_id, table_name=table_id,
                                  column_types=column_types, load_mode='WRITE_TRUNCATE')





def pagamentos_loading():
    dataset_id = 'ft_tables'
    table_id = 'ft_Pagamentos'
    df = model_pagamentos()
    column_types = generate_column_types(df)
    bq.load_dataframe_to_bigquery(dataframe=df, dataset_id=dataset_id, table_name=table_id,
                                  column_types=column_types, load_mode='WRITE_TRUNCATE')


def guarda_funcao():
    lojas_loading()
    maquinas_loading()
    atententes_loading()
    pontoVenda_loading()
    meioPagamento_loading()
    material_loading()
    modoVenda_loading()
    turno_loading()
    operacao_loading()
    rede_loading()
    pagamentos_loading()
rede_loading()