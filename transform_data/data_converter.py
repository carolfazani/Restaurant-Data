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
    def to_boolean(value: bool) -> bool:
        """Converte um valor para booleano."""
        return bool(value)

    @staticmethod
    def to_str(value: any) -> str:
        """Converte qualquer valor para uma string, retornando uma string vazia se for None."""
        return str(value) if value is not None else ""

    @staticmethod
    def to_datetime_with_seconds(timestamp: str) -> pd.Timestamp:
        """Converte um timestamp com segundos para o tipo datetime64[ns]."""
        try:
            dt = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
            return pd.Timestamp(dt)
        except (ValueError, TypeError):
            return pd.NaT

    @staticmethod
    def to_datetime_with_timezone(datetime_str: str) -> pd.Timestamp:
        """Converte um datetime com fuso horário para o tipo datetime64[ns]."""
        try:
            datetime_obj = pd.to_datetime(datetime_str, format="%Y-%m-%dT%H:%M:%S%z", utc=True)
            return datetime_obj
        except (ValueError, TypeError):
            return pd.NaT


    @staticmethod
    def to_datetime(data: str) -> pd.Timestamp:
        """Converte uma data no formato '%Y-%m-%d' ou '%Y%m%d' para um objeto datetime64[ns]."""
        try:
            date_obj = datetime.strptime(data, "%Y-%m-%d")
        except ValueError:
            try:
                date_obj = datetime.strptime(data, "%Y%m%d")
            except ValueError:
                return pd.NaT  # Not a Time (NaT) é a representação do pandas para datas inválidas.
        return pd.Timestamp(date_obj)

    @staticmethod
    def to_seconds(value: str) -> pd.Timestamp:
        """Converte um valor de tempo no formato '%H:%M' para um objeto datetime64[ns] com '%H:%M:%S'."""
        try:
            # Converte a string para o formato completo com segundos
            time_with_seconds = datetime.strptime(value, "%H:%M").strftime("%H:%M:%S")
            # Usa pandas para converter a string para datetime64[ns]
            return pd.to_datetime(time_with_seconds, format="%H:%M:%S")
        except ValueError:
            return pd.NaT


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

    @staticmethod
    def to_int(value: str) -> int:
        return int(value)