from database.database_connection import MysqlPython
from extraction_api.item_sales_extraction import item_sales_extraction
from datetime import datetime, timedelta


def insert_itens_venda():
    dt_inicio = '2017-08-30'
    dt_fim = '2023-06-19'

    # Converter as datas de string para objetos datetime
    dt_inicio = datetime.strptime(dt_inicio, '%Y-%m-%d')
    dt_fim = datetime.strptime(dt_fim, '%Y-%m-%d')

    # Definir o número de dias em cada lote
    dias_por_lote = 2

    mysql_python = MysqlPython()  # Crie uma instância da classe MysqlPython

    while dt_inicio <= dt_fim:
        # Obter a data de início do lote
        data_inicio_lote = dt_inicio.strftime('%Y-%m-%d')

        # Obter a data de fim do lote
        data_fim_lote = (dt_inicio + timedelta(days=dias_por_lote - 1)).strftime('%Y-%m-%d')
        print(data_fim_lote)

        # Chamada da função revenue_extraction com o período atual do lote
        itens_venda = item_sales_extraction(dt_inicio=data_inicio_lote, dt_fim=data_fim_lote)
        dt_inicio += timedelta(days=dias_por_lote)

        for item in itens_venda:
            itens_venda = item['data']

            # Preparar a consulta SQL para inserção na tabela ItemVenda
            sql_item_venda = "INSERT IGNORE INTO itemvenda (idItemVenda, redeId, lojaId, quantidade, idMaterial, codMaterial, descricao, valorTotal, valorUnitario, codGrupo, grupoNome, comboId, combo, operacaoId, atendenteId, atendenteNome, motivoCancelamento, pontoVendaId, pontoVendaNome, modoVendaId, modoVendaNome, cancelado, timestampLancamento, dtLancamento, horaLancamento, tipoCancelamento) VALUES "
            # Preparar a consulta SQL para inserção na tabela Consumidor
            sql_consumidor = "INSERT IGNORE INTO consumidor (idItemVenda, documento, tipo) VALUES "

            for item in itens_venda:
                # Tratar valores None e vazios
                item = {
                    key: value if value is not None and value != '' else 0 if isinstance(value, (int, float)) else ""
                    for key, value in item.items()}

                # Tratamento de valores nulos e tipos incorretos para ItemVenda
                item['quantidade'] = float(item['quantidade']) if item['quantidade'] is not None else 0.0
                item['valorTotal'] = float(item['valorTotal']) if item['valorTotal'] is not None else 0.0
                item['valorUnitario'] = float(item['valorUnitario']) if item['valorUnitario'] is not None and item[
                    'valorUnitario'] != '' else 0.0
                item['cancelado'] = 0 if item['cancelado'] is not True or item['cancelado'] == 0 else 1
                item['comboId'] = str(item['comboId']) if item['comboId'] is not None else ""
                item['codMaterial'] = str(item['codMaterial']) if item['codMaterial'] is not None else ""
                item['idMaterial'] = str(item['idMaterial']) if item['idMaterial'] is not None else ""
                item['combo'] = str(item['combo']) if item['combo'] is not None else ""
                item['operacaoId'] = str(item['operacaoId']) if item['operacaoId'] is not None else ""
                item['atendenteId'] = str(item['atendenteId']) if item['atendenteId'] is not None else ""
                item['pontoVendaId'] = str(item['pontoVendaId']) if item['pontoVendaId'] is not None else ""
                item['modoVendaId'] = str(item['modoVendaId']) if item['modoVendaId'] is not None else ""
                item['timestampLancamento'] = datetime.strptime(item['timestampLancamento'], "%Y-%m-%d %H:%M:%S") if \
                item[
                    'timestampLancamento'] is not None and item[
                    'timestampLancamento'] != '' else None
                item['dtLancamento'] = datetime.strptime(item['dtLancamento'], "%Y%m%d").strftime("%Y-%m-%d") if item[
                                                                                                                     'dtLancamento'] is not None else None

                item['horaLancamento'] = datetime.strptime(item['horaLancamento'], "%H:%M").strftime("%H:%M:%S") if \
                item[
                    'horaLancamento'] is not None else None

                # Construir os valores para a consulta de ItemVenda
                values_item_venda = f'''(
                            "{item['idItemVenda']}",
                            "{item['redeId']}",
                            "{item['lojaId']}",
                            {item['quantidade']},
                            '{item['idMaterial']}',
                            {item['codMaterial']},
                            "{item['descricao']}",
                            {item['valorTotal']},
                            {item['valorUnitario']},
                            '{item['codGrupo']}',
                            "{item['grupoNome']}",
                            '{item['comboId']}',
                            "{item['combo']}",
                            "{item['operacaoId']}",
                            "{item['atendenteId']}",
                            "{item['atendenteNome']}",
                            "{item['motivoCancelamento']}",
                            "{item['pontoVendaId']}",
                            "{item['pontoVendaNome']}",
                            "{item['modoVendaId']}",
                            "{item['modoVendaNome']}",
                            {item['cancelado']},
                            "{item['timestampLancamento']}",
                            "{item['dtLancamento']}",
                            "{item['horaLancamento']}",
                            "{item['tipoCancelamento']}"
                        )'''

                # Concatenar a consulta de ItemVenda
                sql_item_venda += values_item_venda + ","

                consumidores = item['consumidores']
                for detalhe in consumidores:
                    # Tratamento de valores nulos e tipos incorretos para Consumidor
                    detalhe['documento'] = str(detalhe['documento']) if detalhe['documento'] is not None else ""
                    detalhe['tipo'] = str(detalhe['tipo']) if detalhe['tipo'] is not None else ""

                    values_consumidor = f'''(
                        "{item['idItemVenda']}",
                        "{detalhe['documento']}",
                        "{detalhe['tipo']}"
                    )'''

                    sql_consumidor += values_consumidor + ","

        # Remover a última vírgula (",") da consulta
        sql_consumidor = sql_consumidor[:-1]
        sql_item_venda = sql_item_venda[:-1]

        # Executar a consulta
        mysql_python.query(sql_item_venda, commit=True)
        mysql_python.query(sql_consumidor, commit=True)


insert_itens_venda()
