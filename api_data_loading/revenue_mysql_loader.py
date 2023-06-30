from database.database_connection import MysqlPython
from extraction_api.revenue_extraction import revenue_extraction
from datetime import datetime, timedelta


def insert_movimento_caixa():
    dt_inicio = '2017-08-30'
    dt_fim = '2023-01-01'

    # Converter as datas de string para objetos datetime
    dt_inicio = datetime.strptime(dt_inicio, '%Y-%m-%d')
    dt_fim = datetime.strptime(dt_fim, '%Y-%m-%d')

    # Definir o número de dias em cada lote
    dias_por_lote = 30

    mysql_python = MysqlPython()  # Crie uma instância da classe MysqlPython

    while dt_inicio <= dt_fim:
        # Obter a data de início do lote
        data_inicio_lote = dt_inicio.strftime('%Y-%m-%d')

        # Obter a data de fim do lote
        data_fim_lote = (dt_inicio + timedelta(days=dias_por_lote - 1)).strftime('%Y-%m-%d')
        print(data_fim_lote)

        # Chamada da função revenue_extraction com o período atual do lote
        movimento_caixa = revenue_extraction(dt_inicio=data_inicio_lote, dt_fim=data_fim_lote)
        dt_inicio += timedelta(days=dias_por_lote)

        for item in movimento_caixa:
            movimento_caixa = item['data']

            # Preparar a consulta SQL para inserção na tabela MovimentoCaixa
            sql_movimento_caixa = "INSERT IGNORE INTO movimento_caixa (idMovimentoCaixa, redeId, lojaId, hora, idAtendente, codAtendente, nomeAtendente, vlDesconto, vlAcrescimo, vlTotalReceber, vlTotalRecebido, vlServicoRecebido, vlConsumacaoRecebido, vlTrocoContravale, vlTrocoDinheiro, vlTrocoRepique, vlTaxaEntrega, vlEntrada, numPessoas, operacaoId, maquinaId, nomeMaquina, caminhoMaquina, turnoId, cancelado, modoVendaId, modoVendaNome, dataContabil) VALUES "

            # Preparar a consulta SQL para inserção na tabela MeiosPagamento
            sql_meios_pagamento = "INSERT IGNORE INTO meios_pagamento (idMovimentoCaixa, id, codigo, nome, redeId, valor, bandeira, tipoCartao, idAtendente, codAtendente, nomeAtendente, autorizacao, nsu, credenciadora) VALUES "

            for item in movimento_caixa:
                # Tratar valores None e vazios
                item = {key: value if value is not None and value != '' else 0 if isinstance(value,
                                                                                             (int, float, str)) else ""
                        for key, value in item.items()}

                # Converter variáveis para tipo float
                item['vlDesconto'] = float(item['vlDesconto']) if item['vlDesconto'] is not None and item[
                    'vlDesconto'] != '' else 0.00
                item['vlAcrescimo'] = float(item['vlAcrescimo']) if item['vlAcrescimo'] is not None and item[
                    'vlAcrescimo'] != '' else 0.00
                item['vlTotalReceber'] = float(item['vlTotalReceber']) if item['vlTotalReceber'] is not None and item[
                    'vlTotalReceber'] != '' else 0.00
                item['vlTotalRecebido'] = float(item['vlTotalRecebido']) if item['vlTotalRecebido'] is not None and \
                                                                            item[
                                                                                'vlTotalRecebido'] != '' else 0.00
                item['vlServicoRecebido'] = float(item['vlServicoRecebido']) if item[
                                                                                    'vlServicoRecebido'] is not None and \
                                                                                item[
                                                                                    'vlServicoRecebido'] != '' else 0.00
                item['vlConsumacaoRecebido'] = float(item['vlConsumacaoRecebido']) if item[
                                                                                          'vlConsumacaoRecebido'] is not None and \
                                                                                      item[
                                                                                          'vlConsumacaoRecebido'] != '' else 0.00
                item['vlTrocoContravale'] = float(item['vlTrocoContravale']) if item[
                                                                                    'vlTrocoContravale'] is not None and \
                                                                                item[
                                                                                    'vlTrocoContravale'] != '' else 0.00
                item['vlTrocoDinheiro'] = float(item['vlTrocoDinheiro']) if item['vlTrocoDinheiro'] is not None and \
                                                                            item[
                                                                                'vlTrocoDinheiro'] != '' else 0.00
                item['vlTrocoRepique'] = float(item['vlTrocoRepique']) if item['vlTrocoRepique'] is not None and item[
                    'vlTrocoRepique'] != '' else 0.00
                item['vlTaxaEntrega'] = float(item['vlTaxaEntrega']) if item['vlTaxaEntrega'] is not None and item[
                    'vlTaxaEntrega'] != '' else 0.00
                item['vlEntrada'] = float(item['vlEntrada']) if item['vlEntrada'] is not None and item[
                    'vlEntrada'] != '' else 0.00
                item['cancelado'] = 0 if item['cancelado'] is not True or item['cancelado'] == 0 else 1

                data_contabil_str = item['dataContabil']
                data_contabil = datetime.strptime(data_contabil_str, "%Y-%m-%dT%H:%M:%S%z")
                # Construir os valores para a consulta de MovimentoCaixa
                values_movimento_caixa = f'''(
                    "{item['idMovimentoCaixa']}",
                    "{item['redeId']}",
                    "{item['lojaId']}",
                    "{item['hora']}",
                    "{item['idAtendente']}",
                    "{item['codAtendente']}",
                    "{item['nomeAtendente']}",
                    {item['vlDesconto']},
                    {item['vlAcrescimo']},
                    {item['vlTotalReceber']},
                    {item['vlTotalRecebido']},
                    {item['vlServicoRecebido']},
                    {item['vlConsumacaoRecebido']},
                    {item['vlTrocoContravale']},
                    {item['vlTrocoDinheiro']},
                    {item['vlTrocoRepique']},
                    {item['vlTaxaEntrega']},
                    {item['vlEntrada']},
                    {item['numPessoas']},
                    "{item['operacaoId']}",
                    "{item['maquinaId']}",
                    "{item['nomeMaquina']}",
                    "{item['caminhoMaquina']}",
                    "{item['turnoId']}",
                    "{item['cancelado']}",
                    "{item['modoVendaId']}",
                    "{item['modoVendaNome']}",
                    "{data_contabil.strftime("%Y-%m-%d %H:%M:%S")}"
                )'''

                # Concatenar a consulta de MovimentoCaixa
                sql_movimento_caixa += values_movimento_caixa + ","

                # Inserir registros na tabela MeiosPagamento
                meios_pagamento = item['meiosPagamento']
                for meio_pagamento in meios_pagamento:
                    # Tratar valores None e vazios
                    item = {
                        key: value if value is not None and value != '' else 0 if isinstance(value,
                                                                                             (int, float, str)) else ""
                        for key, value in item.items()}

                    # Construir os valores para a consulta de MeiosPagamento
                    values_meios_pagamento = f'''(
                        "{item['idMovimentoCaixa']}",
                        "{meio_pagamento['id']}",
                        "{meio_pagamento['codigo']}",
                        "{meio_pagamento['nome']}",
                        "{meio_pagamento['redeId']}",
                        {meio_pagamento['valor']},
                        "{meio_pagamento['bandeira']}",
                        "{meio_pagamento['tipoCartao']}",
                        "{meio_pagamento['idAtendente']}",
                        "{meio_pagamento['codAtendente']}",
                        "{meio_pagamento['nomeAtendente']}",
                        "{meio_pagamento['autorizacao']}",
                        "{meio_pagamento['nsu']}",
                        "{meio_pagamento['credenciadora']}"
                    )'''

                    # Concatenar a consulta de MeiosPagamento
                    sql_meios_pagamento += values_meios_pagamento + ","

            # Remover a última vírgula (",") de cada consulta
            sql_movimento_caixa = sql_movimento_caixa[:-1]
            sql_meios_pagamento = sql_meios_pagamento[:-1]

            # Executar as consultas
            mysql_python.query(sql_movimento_caixa, commit=True, single=True)
            mysql_python.query(sql_meios_pagamento, commit=True, single=True)


insert_movimento_caixa()
