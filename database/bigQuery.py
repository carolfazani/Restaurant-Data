from google.cloud import bigquery

'''pip install google-cloud-bigquery
instalar CLI do Google
gcloud auth application-default login'''

class BigQueryPython:
    def __init__(self, project: str):
        """Inicializa uma instância da classe BigQueryPython.

        Args:
            project (str): O ID do projeto do Google Cloud para conectar ao BigQuery.
        """
        self.project = project
        self.client = None

    def open(self):
        """Abre uma conexão com o BigQuery."""
        try:
            self.client = bigquery.Client(project=self.project)
            print('Conectado ao BigQuery.')
        except Exception as e:
            print('Erro ao acessar o BigQuery:', e)

    def close(self):
        """Fecha a conexão com o BigQuery."""
        print('Conexão com o BigQuery encerrada.')

    def query(self, sql: str):
        """Executa uma consulta SQL no BigQuery.

        Args:
            sql (str): A consulta SQL a ser executada.

        Returns:
            google.cloud.bigquery.table.RowIterator: Os resultados da consulta.
        """
        try:
            if not self.client:
                self.open()

            query_job = self.client.query(sql)
            results = query_job.result()

            print("Consulta executada com sucesso.")
            return results

        except Exception as e:
            print('Erro ao executar a consulta:', e)

        finally:
            self.close()

    def execute(self, sql: str):
        """Executa uma instrução SQL no BigQuery.

        Args:
            sql (str): A instrução SQL a ser executada.
        """
        try:
            if not self.client:
                self.open()

            query_job = self.client.query(sql)
            query_job.result()

            print("Consulta executada com sucesso.")

        except Exception as e:
            print('Erro ao executar a consulta:', e)

        finally:
            self.close()

    def load_dataframe_to_bigquery(self, dataframe, dataset_id: str, table_name: str, load_mode: str, column_types: dict):
        """Carrega um DataFrame para uma tabela do BigQuery.

        Args:
            dataframe (pandas.DataFrame): O DataFrame a ser carregado.
            dataset_id (str): O ID do conjunto de dados do BigQuery.
            table_name (str): O nome da tabela do BigQuery.
            load_mode (str): O modo de carregamento da tabela ("WRITE_TRUNCATE" atualiza valores de linhas existentes, para acrescentar linhas novas usar o "WRITE_APPEND").
            column_types (dict): Um dicionário que mapeia os nomes das colunas para os tipos de dados.

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

            print(f'Dados do DataFrame carregados na tabela {table_name} do BigQuery.')

        except Exception as e:
            print(f"Erro ao carregar DataFrame no BigQuery: {str(e)}")

        finally:
            self.close()
