from database.bigQuery import BigQueryPython
import pandas as pd


class BigQueryTableCreator:
    def __init__(self, project: str,  is_bronze: bool = False):
        """
        Initializes the BigQueryTableCreator class.

        Args:
            project (str): The BigQuery project ID.
        """
        self.project = project
        self.is_bronze = is_bronze
        self.bq = BigQueryPython(project=self.project)

    def create_table(self, dataframe: pd.DataFrame, dataset_id: str, table_name: str, not_null_columns: list = None) -> None:
        """
        Creates a table in BigQuery based on the provided DataFrame.

        Args:
            dataframe (pd.DataFrame): The DataFrame containing table data.
            dataset_id (str): The dataset ID where the table will be created.
            table_name (str): The name of the table to be created.
            not_null_columns (list, optional): List of columns that should be NOT NULL. Defaults to None.
        """

        query = self._generate_create_table_query(dataframe, dataset_id, table_name, not_null_columns)
        self.bq.query(sql=query)

    def _generate_create_table_query(self, dataframe: pd.DataFrame, dataset_id: str, table_name: str,
                                     not_null_columns: list = None) -> str:
        """
        Generates the SQL query to create a table in BigQuery based on the provided DataFrame.

        Args:
            dataframe (pd.DataFrame): The DataFrame containing table data.
            dataset_id (str): The dataset ID where the table will be created.
            table_name (str): The name of the table to be created.
            not_null_columns (list, optional): List of columns that should be NOT NULL. Defaults to None.

        Returns:
            str: The SQL query to create the table.
        """

        query = f"CREATE TABLE `{dataset_id}.{table_name}` (\n"

        for column_name, column_type in dataframe.dtypes.items():
            bq_type = self._map_dtype_to_bq_type(column_type)
            if not_null_columns and column_name in not_null_columns:
                query += f"  `{column_name}` {bq_type} NOT NULL,\n"
            else:
                query += f"  `{column_name}` {bq_type},\n"

        query = query.rstrip(',\n') + "\n)"
        return query

    def _map_dtype_to_bq_type(self, dtype: str) -> str:
        """
        Maps pandas data types to BigQuery supported data types.

        Args:
            dtype (str): The pandas data type.

        Returns:
            str: The corresponding BigQuery supported data type.
        """
        if self.is_bronze:
            return 'STRING'  #If it's bronze, all columns are STRING
        if dtype in ['datetime64[ns]' , 'datetime64[ns, UTC]' , 'datetime.date']:
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