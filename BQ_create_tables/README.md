---

### `BronzeTableCreator` Class

The `BronzeTableCreator` class is responsible for creating bronze-level tables in BigQuery, which typically hold raw data extracted from various sources. 

#### Attributes:
- `project_id` (str): The Google Cloud project ID.
- `chain` (str): The chain identifier for the data.
- `stores` (list): A list of store identifiers.
- `start_date` (str): The start date for the data extraction.
- `end_date` (str): The end date for the data extraction.
- `table_creator` (BigQueryTableCreator): An instance of the `BigQueryTableCreator` class configured for bronze tables.

#### Methods:
- `__init__(self, project_id, chain, stores, start_date, end_date)`: Initializes the `BronzeTableCreator` with the given parameters.
- `create_item_sales_table(self)`: Extracts item sales data and creates the corresponding bronze table in BigQuery.
- `create_revenue_table(self)`: Extracts revenue data and payment methods data, and creates the corresponding bronze tables in BigQuery.
- `run(self)`: Executes the creation of the item sales and revenue tables.

### `GoldTableCreator` Class

The `GoldTableCreator` class is responsible for creating gold-level tables in BigQuery, which typically hold highly processed and refined data, ready for analysis.

#### Attributes:
- `project_id` (str): The Google Cloud project ID.
- `date` (str): The date for which the data is relevant.
- `table_creator` (BigQueryTableCreator): An instance of the `BigQueryTableCreator` class configured for gold tables.

#### Methods:
- `__init__(self, project_id, date)`: Initializes the `GoldTableCreator` with the given parameters.
- `insert_atententes(self)`: Inserts the "dim_atendentes" table into the gold dataset.
- `insert_lojas(self)`: Inserts the "dim_lojas" table into the gold dataset.
- `insert_maquinas(self)`: Inserts the "dim_maquinas" table into the gold dataset.
- `insert_pontoVenda(self)`: Inserts the "dim_pontosVenda" table into the gold dataset.
- `insert_material(self)`: Inserts the "dim_materiais" table into the gold dataset.
- `insert_meioPagamento(self)`: Inserts the "dim_meiosPagamento" table into the gold dataset.
- `insert_modoVenda(self)`: Inserts the "dim_modosVenda" table into the gold dataset.
- `insert_rede(self)`: Inserts the "dim_redes" table into the gold dataset.
- `insert_movimentoCaixa(self)`: Inserts the "ft_movimentoCaixas" table into the gold dataset.
- `insert_itensVenda(self)`: Inserts the "ft_itensVenda" table into the gold dataset.
- `insert_pagamentos(self)`: Inserts the "ft_pagamentos" table into the gold dataset.
- `run(self)`: Executes the insertion of all gold-level tables.

### `SilverTableCreator` Class

The `SilverTableCreator` class is responsible for creating silver-level tables in BigQuery, which typically hold cleaned and transformed data, bridging the raw bronze-level data and the refined gold-level data.

#### Attributes:
- `project_id` (str): The Google Cloud project ID.
- `date` (str): The date for which the data is relevant.
- `table_creator` (BigQueryTableCreator): An instance of the `BigQueryTableCreator` class configured for silver tables.

#### Methods:
- `__init__(self, project_id, date)`: Initializes the `SilverTableCreator` with the given parameters.
- `create_item_sales_table(self)`: Extracts item sales data and creates the corresponding silver table in BigQuery.
- `create_revenue_table(self)`: Extracts revenue data and creates the corresponding silver table in BigQuery.
- `create_payment_methods_table(self)`: Extracts payment methods data and creates the corresponding silver table in BigQuery.
- `run(self)`: Executes the creation of the item sales, revenue, and payment methods tables.

---
