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


def generate_column_types(dataframe):
    column_types = {}
    for column_name, column_type in dataframe.dtypes.items():
        try:
            if column_type == 'datetime64[ns]':
                bq_type = 'DATE'
            elif column_type == 'object':
                bq_type = 'STRING'
            elif column_type == 'int64':
                bq_type = 'INTEGER'
            elif column_type == 'float64':
                bq_type = 'FLOAT64'
            elif column_type == 'bool':
                bq_type = 'BOOL'
            else:
                bq_type = "STRING"  # Fallback to STRING for unknown types
            column_types[column_name] = bq_type
        except:
            print(f'Nome da coluna {column_name}, tipo {dataframe[column_name].dtype}')

    return column_types