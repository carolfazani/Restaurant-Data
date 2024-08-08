
# data_converter.py

# Data Conversion Functions

This module contains various utility functions for converting different data types in Python. The functions are mainly used to convert values from one type to another, such as strings to float, booleans to integers, timestamps to specific date and time formats, among others.

## Class `DataConverter`

The `DataConverter` class contains static methods for converting different data types.

### Function `to_float(value: str) -> float`

Converts a string to a float, returning 0.00 if the value is None or empty.

#### Parameters
- `value` (str): The value to be converted to float.

#### Returns
- `float`: The converted float value. If conversion is not possible, returns 0.00.

#### Example Usage
```python
from clean_data import DataConverter

converted_value = DataConverter.to_float("123.45")
print(converted_value)  # Output: 123.45
```

### Function `to_boolean(value: bool) -> bool`

Converts a value to boolean.

#### Parameters
- `value` (any): The value to be converted to boolean.

#### Returns
- `bool`: The converted boolean value.

#### Example Usage
```python
converted_value = DataConverter.to_boolean(1)
print(converted_value)  # Output: True
```

### Function `to_str(value: any) -> str`

Converts any value to a string, returning an empty string if the value is None.

#### Parameters
- `value` (any): The value to be converted to string.

#### Returns
- `str`: The converted string value.

#### Example Usage
```python
converted_value = DataConverter.to_str(None)
print(converted_value)  # Output: ""
```

### Function `to_datetime_with_seconds(timestamp: str) -> pd.Timestamp`

Converts a timestamp with seconds to datetime64[ns] type.

#### Parameters
- `timestamp` (str): The timestamp string to be converted.

#### Returns
- `pd.Timestamp`: The converted timestamp. If conversion fails, returns pd.NaT.

#### Example Usage
```python
converted_value = DataConverter.to_datetime_with_seconds("2024-07-18 12:34:56")
print(converted_value)  # Output: 2024-07-18 12:34:56
```

### Function `to_datetime_with_timezone(datetime_str: str) -> pd.Timestamp`

Converts a datetime with timezone to datetime64[ns] type.

#### Parameters
- `datetime_str` (str): The datetime string with timezone to be converted.

#### Returns
- `pd.Timestamp`: The converted datetime. If conversion fails, returns pd.NaT.

#### Example Usage
```python
converted_value = DataConverter.to_datetime_with_timezone("2024-07-18T12:34:56+0000")
print(converted_value)  # Output: 2024-07-18
```

### Function `to_datetime(data: str) -> pd.Timestamp`

Converts a date in the format '%Y-%m-%d' or '%Y%m%d' to a datetime64[ns] object.

#### Parameters
- `data` (str): The date string to be converted.

#### Returns
- `pd.Timestamp`: The converted date. If conversion fails, returns pd.NaT.

#### Example Usage
```python
converted_value = DataConverter.to_datetime("2024-07-18")
print(converted_value)  # Output: 2024-07-18
```

### Function `to_seconds(value: str) -> pd.Timestamp`

Converts a time value in the format '%H:%M' to a datetime64[ns] object with '%H:%M:%S'.

#### Parameters
- `value` (str): The time string to be converted.

#### Returns
- `pd.Timestamp`: The converted time. If conversion fails, returns pd.NaT.

#### Example Usage
```python
converted_value = DataConverter.to_seconds("12:34")
print(converted_value)  # Output: 1970-01-01 12:34:00
```

### Function `expand_column(row: dict, column: str) -> pd.DataFrame`

Expands a column from a dictionary into a DataFrame.

#### Parameters
- `row` (dict): The dictionary containing the column to be expanded.
- `column` (str): The column to be expanded.

#### Returns
- `pd.DataFrame`: The expanded DataFrame.

#### Example Usage
```python
data = {"column": [{"key1": "value1"}, {"key2": "value2"}]}
df = DataConverter.expand_column(data, "column")
print(df)
# Output:
#     key1   key2
# 0  value1   NaN
# 1    NaN  value2
```

### Function `clean_column_names(df: pd.DataFrame) -> pd.DataFrame`

Cleans the column names of a DataFrame by replacing non-alphanumeric characters with underscores.

#### Parameters
- `df` (pd.DataFrame): The DataFrame with columns to be cleaned.

#### Returns
- `pd.DataFrame`: The DataFrame with cleaned column names.

#### Example Usage
```python
df = pd.DataFrame({"column-1": [1, 2], "column 2": [3, 4]})
clean_df = DataConverter.clean_column_names(df)
print(clean_df.columns)
# Output: Index(['column_1', 'column_2'], dtype='object')
```

### Function `list_to_str(df: pd.DataFrame) -> pd.DataFrame`

Converts columns containing lists to string in a DataFrame.

#### Parameters
- `df` (pd.DataFrame): The DataFrame with columns to be converted.

#### Returns
- `pd.DataFrame`: The DataFrame with list columns converted to strings.

#### Example Usage
```python
df = pd.DataFrame({"list_column": [[1, 2, 3], [4, 5, 6]]})
converted_df = DataConverter.list_to_str(df)
print(converted_df)
# Output:
#   list_column
# 0    [1, 2, 3]
# 1    [4, 5, 6]
```

### Function `to_int(value: str) -> int`

Converts a string to an integer.

#### Parameters
- `value` (str): The value to be converted to integer.

#### Returns
- `int`: The converted integer value.

#### Example Usage
```python
converted_value = DataConverter.to_int("123")
print(converted_value)  # Output: 123
```
