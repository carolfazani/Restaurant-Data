## Class: `BigQueryPython`

This class provides an interface for interacting with Google BigQuery. It includes methods for opening and closing connections, executing SQL queries, and loading data from a Pandas DataFrame into a BigQuery table.




### Method `__init__(self, project: str)`
Initializes an instance of the `BigQueryPython` class.

### Parameters:
- `project` (str): The Google Cloud project ID to connect to BigQuery.


```python
def __init__(self, project: str):
    self.project = project
    self.client = None
```

### Method  `open(self)`
Opens a connection to BigQuery.


```python
def open(self):
    try:
        self.client = bigquery.Client(project=self.project)
        print('Connected to BigQuery.')
    except Exception as e:
        print('Error accessing BigQuery:', e)
```

### Method `_close(self)`
Closes the connection to BigQuery.


```python
def _close(self):
    print('Connection to BigQuery closed.')
```

### Method  `query(self, sql: str)`
Executes an SQL query in BigQuery.

### Parameters:
- `sql` (str): The SQL query to execute.

### Returns
- `google.cloud.bigquery.table.RowIterator`: The query results.


```python
def query(self, sql: str):
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
```

### Method  `execute(self, sql: str)`
Executes an SQL statement in BigQuery.

### Parameters:
- `sql` (str): The SQL statement to execute.


```python
def execute(self, sql: str):
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
```

### Method `load_dataframe_to_bigquery(self, dataframe, dataset_id: str, table_name: str, load_mode: str, column_types: dict)`
Loads a DataFrame into a BigQuery table.

### Parameters:
- `dataframe` (pandas.DataFrame): The DataFrame to load.
- `dataset_id` (str): The BigQuery dataset ID.
- `table_name` (str): The BigQuery table name.
- `load_mode` (str): The table loading mode ("WRITE_TRUNCATE" updates existing row values, "WRITE_APPEND" appends new rows).
- `column_types` (dict): A dictionary mapping column names to data types.


```python
def load_dataframe_to_bigquery(self, dataframe, dataset_id: str, table_name: str, load_mode: str, column_types: dict):
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
```