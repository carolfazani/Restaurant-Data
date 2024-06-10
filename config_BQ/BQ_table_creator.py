from database.bigQuery import BigQueryPython
import pandas as pd


class BigQueryTableCreator:
    def __init__(self, project: str):
        """
        Inicializa a classe BigQueryTableCreator.

        Args:
            project (str): O ID do projeto do BigQuery.
        """
        self.project = project
        self.bq = BigQueryPython(project=self.project)

    def create_table(self, dataframe: pd.DataFrame, dataset_id: str, table_name: str) -> None:
        """
        Cria uma tabela no BigQuery com base nos dados do DataFrame fornecido.

        Args:
            dataframe (pd.DataFrame): O DataFrame contendo os dados da tabela.
            dataset_id (str): O ID do dataset onde a tabela será criada.
            table_name (str): O nome da tabela a ser criada.
        """
        query = self._generate_create_table_query(dataframe, dataset_id, table_name)
        self.bq.query(sql=query)

    def _generate_create_table_query(self, dataframe: pd.DataFrame, dataset_id: str, table_name: str) -> str:
        """
        Gera a consulta SQL para criar a tabela no BigQuery com base no DataFrame fornecido.

        Args:
            dataframe (pd.DataFrame): O DataFrame contendo os dados da tabela.
            dataset_id (str): O ID do dataset onde a tabela será criada.
            table_name (str): O nome da tabela a ser criada.

        Returns:
            str: A consulta SQL para criar a tabela.
        """
        query = f"CREATE TABLE `{dataset_id}.{table_name}` (\n"

        for column_name, column_type in dataframe.dtypes.items():
            bq_type = self._map_dtype_to_bq_type(column_type)
            query += f"  `{column_name}` {bq_type},\n"

        query = query.rstrip(',\n') + "\n)"
        return query



    def _map_dtype_to_bq_type(self, dtype: str) -> str:
        """
        Mapeia os tipos de dados do pandas para os tipos de dados suportados pelo BigQuery.

        Args:
            dtype (str): O tipo de dados do pandas.

        Returns:
            str: O tipo de dados correspondente suportado pelo BigQuery.
        """
        if dtype == 'datetime64[ns]':
            return 'DATE'
        elif dtype == 'object':
            return 'STRING'
        elif dtype == 'int64':
            return 'INTEGER'
        elif dtype == 'float64':
            return 'FLOAT64'
        elif dtype == 'bool':
            return 'BOOL'
        else:
            return 'STRING'


'''Para criar as tabelas bronze, usar esse schema
if dtype == 'datetime64[ns]':
    return 'STRING'
elif dtype == 'object':
    return 'STRING'
elif dtype == 'int64':
    return 'STRING'
elif dtype == 'float64':
    return 'STRING'
elif dtype == 'bool':
    return 'STRING'
else:
    return 'STRING
'''