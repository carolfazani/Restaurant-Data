def generate_column_types_bronze(dataframe):
    column_types = {}
    for column_name, column_type in dataframe.dtypes.items():
        try:
            if column_type == 'datetime64[ns]':
                bq_type = 'STRING'
            elif column_type == 'object':
                bq_type = 'STRING'
            elif column_type == 'int64':
                bq_type = 'STRING'
            elif column_type == 'float64':
                bq_type = 'STRING'
            elif column_type == 'bool':
                bq_type = 'STRING'
            else:
                bq_type = "STRING"  # Fallback to STRING for unknown types
            column_types[column_name] = bq_type
        except:
            print(f'Nome da coluna {column_name}, tipo {dataframe[column_name].dtype}')

    return column_types


def generate_column_types(dataframe, not_null_columns=None):
    """
    Gera o esquema das colunas do DataFrame para o BigQuery, incluindo a condição NOT NULL onde aplicável.

    Args:
        dataframe (pd.DataFrame): O DataFrame contendo os dados da tabela.
        not_null_columns (list, optional): Lista das colunas que devem ser NOT NULL. Defaults to None.

    Returns:
        dict: Um dicionário com o nome das colunas e seus tipos de dados para o BigQuery, incluindo NOT NULL onde aplicável.
    """
    if not_null_columns is None:
        not_null_columns = []

    column_types = {}
    for column_name, column_type in dataframe.dtypes.items():
        try:
            # Map pandas types to BigQuery types
            if column_type == 'datetime64[ns]':
                bq_type = 'DATE'
            elif column_type == 'object':
                bq_type = 'STRING'
            elif column_type == 'int64':
                bq_type = 'INTEGER'
            elif column_type == 'float64':
                bq_type = 'FLOAT'
            elif column_type == 'bool':
                bq_type = 'BOOL'
            else:
                bq_type = "STRING"

            mode = "NULLABLE"
            if column_name in not_null_columns:
                mode = "REQUIRED"

            column_types[column_name] = {'type': bq_type, 'mode': mode}
        except Exception as e:
            print(f'Erro ao processar a coluna {column_name}, tipo {dataframe[column_name].dtype}: {e}')

    return column_types

    return column_types


