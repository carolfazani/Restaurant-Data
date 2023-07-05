from database.database_connection import MysqlPython
from extraction_colibri.item_sales_extraction import item_sales_extraction
from transform_data.clean_data import *
from datetime import datetime, timedelta


def insert_itens_venda():

    dt_atual = datetime.today().date()
    dt_anterior = (dt_atual - timedelta(days=1)).strftime('%Y-%m-%d')

    mysql_python = MysqlPython()

    itens_venda = item_sales_extraction(dt_anterior, dt_atual)

    sql_item_venda = "INSERT IGNORE INTO itemvenda (idItemVenda, redeId, lojaId, quantidade, idMaterial, codMaterial, descricao, valorTotal, valorUnitario, codGrupo, grupoNome, comboId, combo, operacaoId, atendenteId, atendenteNome, motivoCancelamento, pontoVendaId, pontoVendaNome, modoVendaId, modoVendaNome, cancelado, timestampLancamento, dtLancamento, horaLancamento, tipoCancelamento) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

    values_item_venda = []

    for item_venda in itens_venda:
        venda = item_venda['data']
        for item in venda:
            values = (
                convert_str(item.get('idItemVenda')),
                convert_str(item.get('redeId')),
                convert_str(item.get('lojaId')),
                convert_float(item.get('quantidade')),
                convert_str(item.get('idMaterial')),
                convert_str(item.get('codMaterial')),
                convert_str(item.get('descricao')),
                convert_float(item.get('valorTotal')),
                convert_float(item.get('valorUnitario')),
                convert_str(item.get('codGrupo')),
                convert_str(item.get('grupoNome')),
                convert_str(item.get('comboId')),
                convert_str(item.get('combo')),
                convert_str(item.get('operacaoId')),
                convert_str(item.get('atendenteId')),
                convert_str(item.get('atendenteNome')),
                convert_str(item.get('motivoCancelamento')),
                convert_str(item.get('pontoVendaId')),
                convert_str(item.get('pontoVendaNome')),
                convert_str(item.get('modoVendaId')),
                convert_str(item.get('modoVendaNome')),
                convert_bolean(item.get('cancelado')),
                convert_datetime_with_seconds(item.get('timestampLancamento')),
                convert_datetime(item.get('dtLancamento')),
                convert_seconds(item.get('horaLancamento')),
                convert_str(item.get('tipoCancelamento'))
            )
            values_item_venda.append(values)


    # Executar a consulta de ItemVenda
    mysql_python.query(sql_item_venda, params= values_item_venda, commit=True)

insert_itens_venda()