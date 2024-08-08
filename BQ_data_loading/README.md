

### `BronzeDataPipeline` Class

The `BronzeDataPipeline` class is responsible for loading raw data into bronze-level tables in BigQuery. This data is typically in its most raw form and is the first step in the data pipeline.

#### Attributes:
- `project_id` (str): The Google Cloud project ID.
- `chain` (str): The chain identifier for the data.
- `stores` (list): A list of store identifiers.
- `start_date` (str): The start date for the data extraction.
- `end_date` (str): The end date for the data extraction.
- `bq` (BigQueryPython): An instance of the `BigQueryPython` class for interacting with BigQuery.

#### Methods:
- `__init__(self, project_id, chain, stores, start_date, end_date)`: Initializes the `BronzeDataPipeline` with the given parameters.
- `insert_item_sales(self)`: Extracts item sales data, converts it to string type, generates column types, and loads it into the bronze dataset in BigQuery.
- `insert_revenue(self)`: Extracts revenue and payment methods data, converts them to string type, generates column types, and loads them into the bronze dataset in BigQuery.
- `run(self)`: Executes the insertion of item sales and revenue data into the bronze dataset.

### `GoldDataPipeline` Class

The `GoldDataPipeline` class is responsible for loading highly processed and refined data into gold-level tables in BigQuery, ready for analysis.

#### Attributes:
- `project_id` (str): The Google Cloud project ID.
- `date` (str): The date for which the data is relevant.
- `bq` (BigQueryPython): An instance of the `BigQueryPython` class for interacting with BigQuery.

#### Methods:
- `__init__(self, project_id, date)`: Initializes the `GoldDataPipeline` with the given parameters.
- `insert_atententes(self)`: Inserts the "dim_atendentes" table into the gold dataset in BigQuery.
- `insert_lojas(self)`: Inserts the "dim_lojas" table into the gold dataset in BigQuery.
- `insert_maquinas(self)`: Inserts the "dim_maquinas" table into the gold dataset in BigQuery.
- `insert_pontoVenda(self)`: Inserts the "dim_pontosVenda" table into the gold dataset in BigQuery.
- `insert_material(self)`: Inserts the "dim_materiais" table into the gold dataset in BigQuery.
- `insert_meioPagamento(self)`: Inserts the "dim_meiosPagamento" table into the gold dataset in BigQuery.
- `insert_modoVenda(self)`: Inserts the "dim_modosVenda" table into the gold dataset in BigQuery.
- `insert_rede(self)`: Inserts the "dim_redes" table into the gold dataset in BigQuery.
- `insert_movimentoCaixa(self)`: Inserts the "ft_movimentoCaixas" table into the gold dataset in BigQuery.
- `insert_itensVenda(self)`: Inserts the "ft_itensVenda" table into the gold dataset in BigQuery.
- `insert_pagamentos(self)`: Inserts the "ft_pagamentos" table into the gold dataset in BigQuery.
- `run(self)`: Executes the insertion of all gold-level tables into the dataset.

### `SilverDataPipeline` Class

The `SilverDataPipeline` class is responsible for loading cleaned and transformed data into silver-level tables in BigQuery, acting as a bridge between raw bronze-level data and refined gold-level data.

#### Attributes:
- `project_id` (str): The Google Cloud project ID.
- `date` (str): The date for which the data is relevant.
- `bq` (BigQueryPython): An instance of the `BigQueryPython` class for interacting with BigQuery.

#### Methods:
- `__init__(self, project_id, date)`: Initializes the `SilverDataPipeline` with the given parameters.
- `insert_item_sales(self)`: Extracts item sales data, generates column types, and loads it into the silver dataset in BigQuery.
- `insert_revenue(self)`: Extracts revenue data, generates column types, and loads it into the silver dataset in BigQuery.
- `insert_payment_methods(self)`: Extracts payment methods data, generates column types, and loads it into the silver dataset in BigQuery.
- `run(self)`: Executes the insertion of item sales, revenue, and payment methods data into the silver dataset.

---
