from transform_data.data_converter import *
from database.bigQuery import BigQueryPython
import pandas as pd


class SilverModelData:
    def __init__(self, project_id: str, date: str):
        self.project_id = project_id
        self.bq = BigQueryPython(project=project_id)
        self.date = date

    def extract_item_sales(self) -> pd.DataFrame:
        query = f""" SELECT * FROM `team-analytics-datalake.bronze.item_sales` WHERE extraction_date = '{self.date}' """
        query_job = self.bq.query(query)
        sale_list = []
        for sale in query_job:
            sale_dict = dict(sale)
            sale_dict['idItemVenda'] = DataConverter.to_str(sale_dict['idItemVenda'])
            sale_dict['redeId'] = DataConverter.to_str(sale_dict['redeId'])
            sale_dict['lojaId'] = DataConverter.to_str(sale_dict['lojaId'])
            sale_dict['quantidade'] = DataConverter.to_float(sale_dict['quantidade'])
            sale_dict['idMaterial'] = DataConverter.to_str(sale_dict['idMaterial'])
            sale_dict['codMaterial'] = DataConverter.to_str(sale_dict['codMaterial'])
            sale_dict['descricao'] = DataConverter.to_str(sale_dict['descricao'])
            sale_dict['valorTotal'] = DataConverter.to_float(sale_dict['valorTotal'])
            sale_dict['valorUnitario'] = DataConverter.to_float(sale_dict['valorUnitario'])
            sale_dict['codGrupo'] = DataConverter.to_str(sale_dict['codGrupo'])
            sale_dict['grupoNome'] = DataConverter.to_str(sale_dict['grupoNome'])
            sale_dict['comboId'] = DataConverter.to_str(sale_dict['comboId'])
            sale_dict['combo'] = DataConverter.to_str(sale_dict['combo'])
            sale_dict['operacaoId'] = DataConverter.to_str(sale_dict['operacaoId'])
            sale_dict['atendenteId'] = DataConverter.to_str(sale_dict['atendenteId'])
            sale_dict['atendenteNome'] = DataConverter.to_str(sale_dict['atendenteNome'])
            sale_dict['motivoCancelamento'] = DataConverter.to_str(sale_dict['motivoCancelamento'])
            sale_dict['pontoVendaId'] = DataConverter.to_str(sale_dict['pontoVendaId'])
            sale_dict['pontoVendaNome'] = DataConverter.to_str(sale_dict['pontoVendaNome'])
            sale_dict['modoVendaId'] = DataConverter.to_str(sale_dict['modoVendaId'])
            sale_dict['modoVendaNome'] = DataConverter.to_str(sale_dict['modoVendaNome'])
            sale_dict['cancelado'] = DataConverter.to_boolean(sale_dict['cancelado'])
            sale_dict['timestampLancamento'] = DataConverter.to_datetime_with_seconds(sale_dict['timestampLancamento'])
            sale_dict['dtLancamento'] = DataConverter.to_datetime(sale_dict['dtLancamento'])
            sale_dict['horaLancamento'] = DataConverter.to_str(sale_dict['horaLancamento'])
            sale_dict['tipoCancelamento'] = DataConverter.to_str(sale_dict['tipoCancelamento'])
            sale_dict['extraction_date'] = DataConverter.to_datetime(sale_dict['extraction_date'])
            sale_list.append(sale_dict)
        silver_dataframe = pd.DataFrame(sale_list)
        return silver_dataframe

    def extract_revenue(self):
        query = f""" SELECT * FROM `team-analytics-datalake.bronze.revenue` WHERE extraction_date = '{self.date}' """
        query_job = self.bq.query(query)
        revenue_list = []
        for revenue in query_job:
            revenue_dict =  dict(revenue)
            revenue_dict['idMovimentoCaixa'] = DataConverter.to_str(revenue_dict['idMovimentoCaixa'])
            revenue_dict['redeId'] = DataConverter.to_str(revenue_dict['redeId'])
            revenue_dict['rede'] = DataConverter.to_str(revenue_dict['rede'])
            revenue_dict['lojaId'] = DataConverter.to_str(revenue_dict['lojaId'])
            revenue_dict['loja'] = DataConverter.to_str(revenue_dict['loja'])
            revenue_dict['hora'] = DataConverter.to_int(revenue_dict['hora'])
            revenue_dict['idAtendente'] = DataConverter.to_str(revenue_dict['idAtendente'])
            revenue_dict['codAtendente'] = DataConverter.to_str(revenue_dict['codAtendente'])
            revenue_dict['nomeAtendente'] = DataConverter.to_str(revenue_dict['nomeAtendente'])
            revenue_dict['vlDesconto'] = DataConverter.to_float(revenue_dict['vlDesconto'])
            revenue_dict['vlAcrescimo'] = DataConverter.to_float(revenue_dict['vlAcrescimo'])
            revenue_dict['vlTotalReceber'] = DataConverter.to_float(revenue_dict['vlTotalReceber'])
            revenue_dict['vlTotalRecebido'] = DataConverter.to_float(revenue_dict['vlTotalRecebido'])
            revenue_dict['vlServicoRecebido'] = DataConverter.to_float(revenue_dict['vlServicoRecebido'])
            revenue_dict['vlConsumacaoRecebido'] = DataConverter.to_float(revenue_dict['vlConsumacaoRecebido'])
            revenue_dict['vlTrocoContravale'] = DataConverter.to_float(revenue_dict['vlTrocoContravale'])
            revenue_dict['vlTrocoDinheiro'] = DataConverter.to_float(revenue_dict['vlTrocoDinheiro'])
            revenue_dict['vlTrocoRepique'] = DataConverter.to_float(revenue_dict['vlTrocoRepique'])
            revenue_dict['vlTaxaEntrega'] = DataConverter.to_float(revenue_dict['vlTaxaEntrega'])
            revenue_dict['vlEntrada'] = DataConverter.to_float(revenue_dict['vlEntrada'])
            revenue_dict['numPessoas'] = DataConverter.to_int(revenue_dict['numPessoas'])
            revenue_dict['operacaoId'] = DataConverter.to_str(revenue_dict['operacaoId'])
            revenue_dict['maquinaId'] = DataConverter.to_str(revenue_dict['maquinaId'])
            revenue_dict['nomeMaquina'] = DataConverter.to_str(revenue_dict['nomeMaquina'])
            revenue_dict['caminhoMaquina'] = DataConverter.to_str(revenue_dict['caminhoMaquina'])
            revenue_dict['meiosPagamento'] = DataConverter.to_str(revenue_dict['meiosPagamento'])
            revenue_dict['turnoId'] = DataConverter.to_str(revenue_dict['turnoId'])
            revenue_dict['consumidores'] = DataConverter.to_str(revenue_dict['consumidores'])
            revenue_dict['cancelado'] = DataConverter.to_boolean(revenue_dict['cancelado'])
            revenue_dict['modoVendaId'] = DataConverter.to_str(revenue_dict['modoVendaId'])
            revenue_dict['modoVendaNome'] = DataConverter.to_str(revenue_dict['modoVendaNome'])
            revenue_dict['tipoId'] = DataConverter.to_str(revenue_dict['tipoId'])
            revenue_dict['tipoDesc'] = DataConverter.to_str(revenue_dict['tipoDesc'])
            revenue_dict['clientes'] = DataConverter.to_str(revenue_dict['clientes'])
            revenue_dict['dataContabil'] = DataConverter.to_datetime_with_timezone(revenue_dict['dataContabil'])
            revenue_dict['extraction_date'] = DataConverter.to_datetime(revenue_dict['extraction_date'])
            revenue_dict['statusComprovante_numero'] = DataConverter.to_str(revenue_dict['statusComprovante_numero'])
            revenue_dict['statusComprovante_chave'] = DataConverter.to_str(revenue_dict['statusComprovante_chave'])
            revenue_dict['statusComprovante_status'] = DataConverter.to_str(revenue_dict['statusComprovante_status'])
            revenue_dict['statusComprovante_ressalva'] = DataConverter.to_str(revenue_dict['statusComprovante_ressalva'])
            revenue_dict['statusComprovante'] = DataConverter.to_str(revenue_dict['statusComprovante'])
            revenue_list.append(revenue_dict)
        silver_dataframe = pd.DataFrame(revenue_list)
        print(revenue_dict['dataContabil'])
        return silver_dataframe

    def extract_payment_methods(self) -> pd.DataFrame:
         query = f""" SELECT * FROM `team-analytics-datalake.bronze.payment_methods` WHERE extraction_date = '{self.date}' """
         query_job = self.bq.query(query)
         payment_methods_list = []
         for payment_methods in query_job:
             payment_methods_dict = dict(payment_methods)
             payment_methods_dict['id'] = DataConverter.to_str(payment_methods_dict['id'])
             payment_methods_dict['codigo'] = DataConverter.to_str(payment_methods_dict['codigo'])
             payment_methods_dict['nome'] = DataConverter.to_str(payment_methods_dict['nome'])
             payment_methods_dict['redeId'] = DataConverter.to_str(payment_methods_dict['redeId'])
             payment_methods_dict['valor'] = DataConverter.to_float(payment_methods_dict['valor'])
             payment_methods_dict['bandeira'] = DataConverter.to_str(payment_methods_dict['bandeira'])
             payment_methods_dict['tipoCartao'] = DataConverter.to_str(payment_methods_dict['tipoCartao'])
             payment_methods_dict['idAtendente'] = DataConverter.to_str(payment_methods_dict['idAtendente'])
             payment_methods_dict['codAtendente'] = DataConverter.to_str(payment_methods_dict['codAtendente'])
             payment_methods_dict['nomeAtendente'] = DataConverter.to_str(payment_methods_dict['nomeAtendente'])
             payment_methods_dict['autorizacao'] = DataConverter.to_str(payment_methods_dict['autorizacao'])
             payment_methods_dict['nsu'] = DataConverter.to_str(payment_methods_dict['nsu'])
             payment_methods_dict['credenciadora'] = DataConverter.to_str(payment_methods_dict['credenciadora'])
             payment_methods_dict['turnoId'] = DataConverter.to_str(payment_methods_dict['turnoId'])
             payment_methods_dict['cliente'] = DataConverter.to_str(payment_methods_dict['cliente'])
             payment_methods_dict['extraction_date'] = DataConverter.to_datetime(payment_methods_dict['extraction_date'])
             payment_methods_dict['idMovimentoCaixa'] = DataConverter.to_str(payment_methods_dict['idMovimentoCaixa'])
             payment_methods_list.append(payment_methods_dict)
         silver_dataframe = pd.DataFrame(payment_methods_list)
         return silver_dataframe

'''
project_id = os.environ['project_id']
date = datetime.now().strftime('%Y-%m-%d')
SilverModelData = SilverModelData(date = date, project_id= project_id)
SilverModelData.extract_item_sales()'''
