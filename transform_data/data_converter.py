from datetime import datetime
import pandas as pd
import re

class DataConverter:
    @staticmethod
    def to_float(value: str) -> float:
        """Converte uma string para float, retornando 0.00 se for None ou vazia."""
        try:
            return float(value) if value else 0.00
        except ValueError:
            return 0.00

    @staticmethod
    def to_boolean(value: bool) -> int:
        """Converte um booleano para 0 ou 1."""
        return 1 if value else 0

    @staticmethod
    def to_str(value: any) -> str:
        """Converte qualquer valor para uma string, retornando uma string vazia se for None."""
        return str(value) if value is not None else ""

    @staticmethod
    def to_datetime_with_seconds(timestamp: str) -> str:
        """Converte um timestamp com segundos para o formato '%Y-%m-%d %H:%M:%S'."""
        try:
            dt = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except (ValueError, TypeError):
            return ""

    @staticmethod
    def to_datetime_with_timezone(datetime_str: str) -> str:
        """Converte um datetime com fuso horário para o formato '%Y-%m-%d %H:%M:%S'."""
        try:
            datetime_obj = datetime.strptime(datetime_str, "%Y-%m-%dT%H:%M:%S%z")
            return datetime_obj.strftime("%Y-%m-%d %H:%M:%S")
        except (ValueError, TypeError):
            return ""

    @staticmethod
    def to_datetime(data: str) -> str:
        """Converte uma data no formato '%Y-%m-%d' ou '%Y%m%d' para '%Y-%m-%d'."""
        try:
            date_obj = datetime.strptime(data, "%Y-%m-%d")
            return date_obj.strftime("%Y-%m-%d")
        except ValueError:
            try:
                date_obj = datetime.strptime(data, "%Y%m%d")
                return date_obj.strftime("%Y-%m-%d")
            except ValueError:
                return ""

    @staticmethod
    def to_seconds(value: str) -> str:
        """Converte um valor de tempo no formato '%H:%M' para '%H:%M:%S'."""
        try:
            return datetime.strptime(value, "%H:%M").strftime("%H:%M:%S")
        except ValueError:
            return ""

    @staticmethod
    def expand_column(row: dict, column: str) -> pd.DataFrame:
        """Expande uma coluna de um dicionário em um DataFrame."""
        return pd.DataFrame(row[column])

    @staticmethod
    def clean_column_names(df):
        regex = re.compile(r'[^a-zA-Z0-9_]')
        def substituir(match):
            return '_'
        novos_nomes = [regex.sub(substituir, col) for col in df.columns]
        df.columns = novos_nomes
        return df

    @staticmethod
    def list_to_str(df):
        for column in df.columns:
            if isinstance(df[column].iloc[0], list):
                df[column] = df[column].astype(str)
        return df
