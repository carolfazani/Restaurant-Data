# readme model data

Here's how you might structure the documentation for the provided Python code. This documentation covers the `BronzeModelData`, `SilverModelData`, and `GoldModelData` classes, outlining their purpose and functionality.

---

## Documentation for Data Model Classes

### `BronzeModelData`

**Purpose:**

Handles the extraction of sales and revenue data from an external API and performs initial data transformation.

**Methods:**

- **`extract_item_sales(start_date: str, end_date: str, chain: list, stores: list) -> pd.DataFrame`**
    
    Extracts item sales data from the API, processes the data, and returns it as a Pandas DataFrame. The method normalizes JSON data and cleans column names.
    
- **`extract_revenue(start_date: str, end_date: str, chain: list, stores: list) -> tuple`**
    
    Extracts revenue data from the API, including payment methods. The method processes and normalizes the revenue data and payment methods separately, returning both as Pandas DataFrames.
    

### `SilverModelData`

**Purpose:**

Extracts and transforms data from the `Bronze` layer in BigQuery to prepare it for further processing in the `Gold` layer.

**Constructor:**

- **`__init__(self, project_id: str, date: str)`**
Initializes the `SilverModelData` instance with a Google Cloud project ID and the date for querying data.

**Methods:**

- **`extract_item_sales() -> pd.DataFrame`**
    
    Queries and extracts item sales data from the `silver` layer in BigQuery, cleans the data, and returns it as a Pandas DataFrame.
    
- **`extract_revenue() -> pd.DataFrame`**
    
    Queries and extracts revenue data from the `silver` layer in BigQuery, processes the data, and returns it as a Pandas DataFrame.
    
- **`extract_payment_methods() -> pd.DataFrame`**
    
    Queries and extracts payment methods data from the `silver` layer in BigQuery, processes the data, and returns it as a Pandas DataFrame.
    
- **`model_atendentes() -> pd.DataFrame`**
    
    Extracts distinct data about attendants from the `silver` layer.
    
- **`model_lojas() -> pd.DataFrame`**
    
    Extracts distinct data about stores from the `silver` layer.
    
- **`model_maquinas() -> pd.DataFrame`**
    
    Extracts distinct data about machines from the `silver` layer.
    
- **`model_rede() -> pd.DataFrame`**
    
    Extracts distinct data about networks from the `silver` layer.
    
- **`model_modoVenda() -> pd.DataFrame`**
    
    Extracts distinct data about sales modes from the `silver` layer.
    
- **`model_meioPagamento() -> pd.DataFrame`**
    
    Extracts distinct data about payment methods from the `silver` layer.
    
- **`model_material() -> pd.DataFrame`**
    
    Extracts distinct data about materials from the `silver` layer.
    
- **`model_grupo() -> pd.DataFrame`**
    
    Extracts distinct data about groups from the `silver` layer.
    
- **`model_combo() -> pd.DataFrame`**
    
    Extracts distinct data about combos from the `silver` layer.
    
- **`model_pontoVenda() -> pd.DataFrame`**
    
    Extracts distinct data about sales points from the `silver` layer.
    
- **`model_itensVenda() -> pd.DataFrame`**
    
    Extracts detailed item sales data from the `silver` layer.
    
- **`model_MovimentoCaixa() -> pd.DataFrame`**
    
    Extracts detailed revenue movement data from the `silver` layer.
    
- **`model_pagamentos() -> pd.DataFrame`**
    
    Extracts detailed payment data from the `silver` layer.
    

### `GoldModelData`

**Purpose:**

Processes and models data extracted from the `Silver` layer to prepare it for final analysis or reporting.

**Constructor:**

- **`__init__(self, project_id: str, date: str)`**
Initializes the `GoldModelData` instance with a Google Cloud project ID and the date for querying data.

**Methods:**

- **`extract_item_sales() -> pd.DataFrame`**
    
    Queries item sales data from the `gold` layer in BigQuery, processes the data, and returns it as a Pandas DataFrame.
    
- **`extract_payment_methods() -> pd.DataFrame`**
    
    Queries payment methods data from the `gold` layer in BigQuery, processes the data, and returns it as a Pandas DataFrame.
    
- **`extract_revenue() -> pd.DataFrame`**
    
    Queries revenue data from the `gold` layer in BigQuery, processes the data, and returns it as a Pandas DataFrame.
    
- **`model_atendentes() -> pd.DataFrame`**
    
    Extracts distinct data about attendants from the `gold` layer.
    
- **`model_lojas() -> pd.DataFrame`**
    
    Extracts distinct data about stores from the `gold` layer.
    
- **`model_maquinas() -> pd.DataFrame`**
    
    Extracts distinct data about machines from the `gold` layer.
    
- **`model_rede() -> pd.DataFrame`**
    
    Extracts distinct data about networks from the `gold` layer.
    
- **`model_modoVenda() -> pd.DataFrame`**
    
    Extracts distinct data about sales modes from the `gold` layer.
    
- **`model_meioPagamento() -> pd.DataFrame`**
    
    Extracts distinct data about payment methods from the `gold` layer.
    
- **`model_material() -> pd.DataFrame`**
    
    Extracts distinct data about materials from the `gold` layer.
    
- **`model_grupo() -> pd.DataFrame`**
    
    Extracts distinct data about groups from the `gold` layer.
    
- **`model_combo() -> pd.DataFrame`**
    
    Extracts distinct data about combos from the `gold` layer.
    
- **`model_pontoVenda() -> pd.DataFrame`**
    
    Extracts distinct data about sales points from the `gold` layer.
    
- **`model_itensVenda() -> pd.DataFrame`**
    
    Extracts detailed item sales data from the `gold` layer.
    
- **`model_MovimentoCaixa() -> pd.DataFrame`**
    
    Extracts detailed revenue movement data from the `gold` layer.
    
- **`model_pagamentos() -> pd.DataFrame`**
    
    Extracts detailed payment data from the `gold` layer.
    

---

