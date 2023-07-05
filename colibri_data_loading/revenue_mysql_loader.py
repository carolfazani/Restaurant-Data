from database.database_connection import MysqlPython
from extraction_colibri.revenue_extraction import revenue_extraction
from transform_data.clean_data import *
from datetime import datetime, timedelta



def insert_movimento_caixa():


    dt_atual = datetime.today().date()
    #dt_anterior = (dt_atual - timedelta(days=1)).strftime('%Y-%m-%d')
    dt_anterior = '2017-08-01'

    mysql_python = MysqlPython()

    movimento_caixa = revenue_extraction(dt_anterior, dt_atual)

    sql_movimento_caixa = "INSERT IGNORE INTO movimento_caixa (idMovimentoCaixa, redeId, lojaId, hora, idAtendente, codAtendente, nomeAtendente, vlDesconto, vlAcrescimo, vlTotalReceber, vlTotalRecebido, vlServicoRecebido, vlConsumacaoRecebido, vlTrocoContravale, vlTrocoDinheiro, vlTrocoRepique, vlTaxaEntrega, vlEntrada, numPessoas, operacaoId, maquinaId, nomeMaquina, caminhoMaquina, turnoId, cancelado, modoVendaId, modoVendaNome, dataContabil) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    sql_meios_pagamento = "INSERT IGNORE INTO meios_pagamento (idMovimentoCaixa, id, codigo, nome, redeId, valor, bandeira, tipoCartao, idAtendente, codAtendente, nomeAtendente, autorizacao, nsu, credenciadora) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

    params_movimento_caixa = []
    params_meios_pagamento = []

    for item in movimento_caixa:
        movimento_item = item['data']
        for movimento in movimento_item:
            values_movimento_caixa = (
                convert_str(movimento.get('idMovimentoCaixa')),
                convert_str(movimento.get('redeId')),
                convert_str(movimento.get('lojaId')),
                movimento['hora'],
                convert_str(movimento.get('idAtendente')),
                convert_str(movimento.get('codAtendente')),
                convert_str(movimento.get('nomeAtendente')),
                convert_float(movimento.get('vlDesconto')),
                convert_float(movimento.get('vlAcrescimo')),
                convert_float(movimento.get('vlTotalReceber')),
                convert_float(movimento.get('vlTotalRecebido')),
                convert_float(movimento.get('vlServicoRecebido')),
                convert_float(movimento.get('vlConsumacaoRecebido')),
                convert_float(movimento.get('vlTrocoContravale')),
                convert_float(movimento.get('vlTrocoDinheiro')),
                convert_float(movimento.get('vlTrocoRepique')),
                convert_float(movimento.get('vlTaxaEntrega')),
                convert_float(movimento.get('vlEntrada')),
                movimento['numPessoas'],
                convert_str(movimento.get('operacaoId')),
                convert_str(movimento.get('maquinaId')),
                convert_str(movimento.get('nomeMaquina')),
                convert_str(movimento.get('caminhoMaquina')),
                convert_str(movimento.get('turnoId')),
                convert_bolean(movimento.get('cancelado')),
                convert_str(movimento.get('modoVendaId')),
                convert_str(movimento.get('modoVendaNome')),
                movimento['dataContabil']
            )
            params_movimento_caixa.append(values_movimento_caixa)


            meios_pagamento = movimento.get('meiosPagamento', [])
            for meio_pagamento in meios_pagamento:
                values_meios_pagamento = (
                    convert_str(movimento.get('idMovimentoCaixa')),
                    convert_str(meio_pagamento.get('id')),
                    convert_str(meio_pagamento.get('codigo')),
                    convert_str(meio_pagamento.get('nome')),
                    convert_str(meio_pagamento.get('redeId')),
                    convert_float(meio_pagamento.get('valor')),
                    convert_str(meio_pagamento.get('bandeira')),
                    convert_str(meio_pagamento.get('tipoCartao')),
                    convert_str(meio_pagamento.get('idAtendente')),
                    convert_str(meio_pagamento.get('codAtendente')),
                    convert_str(meio_pagamento.get('nomeAtendente')),
                    convert_str(meio_pagamento.get('autorizacao')),
                    convert_str(meio_pagamento.get('nsu')),
                    convert_str(meio_pagamento.get('credenciadora'))
                )
                params_meios_pagamento.append(values_meios_pagamento)

        # Executar as consultas
    mysql_python.query(sql_movimento_caixa, params=params_movimento_caixa, commit=True)
    mysql_python.query(sql_meios_pagamento, params=params_meios_pagamento, commit=True)

insert_movimento_caixa()
