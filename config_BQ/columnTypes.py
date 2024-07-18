def generate_column_types(dataframe, not_null_columns=None, is_bronze: bool = False):
    """
    Generates the schema of DataFrame columns for BigQuery, including the NOT NULL condition where applicable.

    Args:
        dataframe (pd.DataFrame): The DataFrame containing table data.
        not_null_columns (list, optional): List of columns that should be NOT NULL. Defaults to None.

    Returns:
        dict: A dictionary with column names and their data types for BigQuery, including NOT NULL where applicable.
    """

    if not_null_columns is None:
        not_null_columns = []

    column_types = {}
    for column_name, column_type in dataframe.dtypes.items():
        try:
            if is_bronze:
                bq_type = 'STRING'  #If it's bronze, all columns are STRING
            if column_type in [ 'datetime64[ns]', 'datetime64[ns, UTC]']:
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
            print(f'Error processing column {column_name}, type {dataframe[column_name].dtype}: {e}')

    return column_types




