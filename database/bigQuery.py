from google.cloud import bigquery

'''pip install google-cloud-bigquery
instalar CLI do Google
gcloud auth application-default login'''

class BigQueryPython:
    def __init__(self, project: str):
        """Initializes an instance of the BigQueryPython class.

        Args:
            project (str): The Google Cloud project ID to connect to BigQuery.
        """
        self.project = project
        self.client = None

    def open(self):
        """Opens a connection to BigQuery."""
        try:
            self.client = bigquery.Client(project=self.project)
            print('Connected to BigQuery.')
        except Exception as e:
            print('Error accessing BigQuery:', e)

    def _close(self):
        """Closes the connection to BigQuery."""
        print('Connection to BigQuery closed.')

    def query(self, sql: str):
        """Executes an SQL query in BigQuery.

        Args:
            sql (str): The SQL query to execute.

        Returns:
            google.cloud.bigquery.table.RowIterator: The query results.
        """

        try:
            if not self.client:
                self.open()

            query_job = self.client.query(sql)
            results = query_job.result()

            print("Query executed successfully.")
            return results

        except Exception as e:
            print('Error executing the query:', e)

        finally:
            self._close()

    def execute(self, sql: str):
        """Executes an SQL statement in BigQuery.

        Args:
            sql (str): The SQL statement to execute.
        """

        try:
            if not self.client:
                self.open()

            query_job = self.client.query(sql)
            query_job.result()

            print("Query executed successfully.")

        except Exception as e:
            print('Error executing the query:', e)

        finally:
            self._close()

    def load_dataframe_to_bigquery(self, dataframe, dataset_id: str, table_name: str, load_mode: str, column_types: dict):
        """Loads a DataFrame into a BigQuery table.

        Args:
            dataframe (pandas.DataFrame): The DataFrame to load.
            dataset_id (str): The BigQuery dataset ID.
            table_name (str): The BigQuery table name.
            load_mode (str): The table loading mode ("WRITE_TRUNCATE" updates existing row values, "WRITE_APPEND" appends new rows).
            column_types (dict): A dictionary mapping column names to data types.
        """

        try:
            if not self.client:
                self.open()

            table_ref = f"{self.client.project}.{dataset_id}.{table_name}"

            job_config = bigquery.LoadJobConfig(autodetect=False)
            job_config.write_disposition = getattr(bigquery.WriteDisposition, load_mode)
            job_config.schema = [bigquery.SchemaField(name, field['type'], field['mode']) for name, field in column_types.items()]

            job = self.client.load_table_from_dataframe(dataframe, table_ref, job_config=job_config)
            job.result()

            print(f'DataFrame data loaded into BigQuery table {table_name}')

        except Exception as e:
            print(f"Error loading DataFrame into BigQuery: {str(e)}")

        finally:
            self._close()
