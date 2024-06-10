from apagar.revenue_extraction import revenue_extraction
from apagar.item_sales_extraction import item_sales_extraction
from transform_data.data_converter import *
import pandas as pd

dt_atual = datetime.today().date()
#dt_anterior = (dt_atual - timedelta(days=1)).strftime('%Y-%m-%d')
dt_anterior = '2024-01-01'
movimento_caixa = revenue_extraction(dt_anterior, dt_atual)
itens_venda = item_sales_extraction(dt_anterior, dt_atual)

def model_atendentes():
    for item in movimento_caixa:
        movimento_item = item['data']
        df = pd.DataFrame(movimento_item)
        df = df[['idAtendente', 'codAtendente', 'nomeAtendente']]
        df = df.drop_duplicates()
    return df

def model_lojas():
    for item in movimento_caixa:
        movimento_item = item['data']
        df = pd.DataFrame(movimento_item)
        df = df[['loja', 'lojaId', 'redeId']]
        df = df.drop_duplicates()
    return df

def model_maquinas():
    for item in movimento_caixa:
        movimento_item = item['data']
        df = pd.DataFrame(movimento_item)
        df = df[['maquinaId', 'nomeMaquina', 'caminhoMaquina']]
        df = df.drop_duplicates()
    return df



def model_rede():
    for item in movimento_caixa:
        movimento_item = item['data']
        df = pd.DataFrame(movimento_item)
        df = df[['redeId']]
        df = df.drop_duplicates()
    return df

def model_operacao():
    for item in movimento_caixa:
        movimento_item = item['data']
        df = pd.DataFrame(movimento_item)
        df = df[['operacaoId']]
        df = df.drop_duplicates()
    return df

def model_turno():
    for item in movimento_caixa:
        movimento_item = item['data']
        df = pd.DataFrame(movimento_item)
        df = df[['turnoId']]
        df = df.drop_duplicates()
    return df

def model_modoVenda():
    for item in movimento_caixa:
        movimento_item = item['data']
        df = pd.DataFrame(movimento_item)
        df = df[['modoVendaId', 'modoVendaNome']]
        df = df.drop_duplicates()
    return df

def model_meioPagamento():
    for item in movimento_caixa:
        movimento_item = item['data']
        df = pd.DataFrame(movimento_item)
        df = pd.concat([expand_column(row, column='meiosPagamento') for index, row in df.iterrows()], ignore_index=True)
        df = df[['id',    'codigo',    'nome',    'redeId',    'valor',    'bandeira',    'tipoCartao',    'idAtendente',    'autorizacao',    'nsu',    'credenciadora',    'turnoId']]
        df = df.drop_duplicates()
        return df


def model_material():
    for item in itens_venda:
        item_venda = item['data']
        df = pd.DataFrame(item_venda)
        df = df[ ['idMaterial',    'codMaterial',    'codMaterialStr',    'descricao',    'valorTotal',    'valorUnitario',    'codGrupo',    'grupoNome',    'comboId',    'combo']]
        df = df.drop_duplicates()
    return df


def model_pontoVenda():
    for item in itens_venda:
        item_venda = item['data']
        df = pd.DataFrame(item_venda)
        df = df[['pontoVendaId', 'pontoVendaNome']]
        df = df.drop_duplicates()
    return df

def model_MovimentoCaixa():
    for item in movimento_caixa:
        movimento_item = item['data']
        df = pd.DataFrame(movimento_item)
        df = df[['idMovimentoCaixa',    'redeId',    'lojaId',    'hora',    'idAtendente',    'vlDesconto',    'vlAcrescimo',    'vlTotalReceber',    'vlTotalRecebido',    'vlServicoRecebido',    'vlConsumacaoRecebido',    'vlTrocoContravale',    'vlTrocoDinheiro',    'vlTrocoRepique',  'vlTaxaEntrega',    'vlEntrada',    'numPessoas',    'operacaoId',    'maquinaId',    'turnoId',    'cancelado',    'modoVendaId',    'dataContabil']]
        df = df.drop_duplicates()
    return df


def model_ItensVenda():
    for item in itens_venda:
        movimento_item = item['data']
        df = pd.DataFrame(movimento_item)
        df = df[['idItemVenda',    'redeId',    'lojaId',    'quantidade',    'idMaterial',    'operacaoId',    'atendenteId',    'pontoVendaId',    'modoVendaId',    'cancelado',    'timestampLancamento',    'dtLancamento',    'horaLancamento',    'tipoCancelamento']]
        df = df.drop_duplicates()
    return df
#achar onde ta essas infos
def model_pagamentos():
    for item in itens_venda:
        movimento_item = item['data']
        df = pd.DataFrame(movimento_item)
        df = df[['idPagamento',    'idMovimentoCaixa',    'statusComprovante',    'numero',    'chave',    'status',    'ressalva',    'meioPagamentoId']]
        df = df.drop_duplicates()
    return df




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