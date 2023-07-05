import os
import mysql.connector
from mysql.connector import Error

class MysqlPython:
    def __init__(self):
        self.mydb = None
        self.cursor = None

    def open(self):
        try:
            self.mydb = mysql.connector.connect(
                host=os.environ.get('MYSQL_HOST'),
                port=os.environ.get('MYSQL_PORT'),
                user=os.environ.get('MYSQL_USER'),
                passwd=os.environ.get('MYSQL_PASSWORD'),
                database=os.environ.get('MYSQL_DATABASE'),
                auth_plugin=os.environ.get('MYSQL_AUTH_PLUGIN')
            )
            self.cursor = self.mydb.cursor()
            print('conectou')

        except Error as e:
            print('Erro ao acessar MySQL:', e)


    def close(self):
        if self.mydb and self.mydb.is_connected():
            self.cursor.close() if self.cursor else None
            self.mydb.close()
            print('Conexão MySQL encerrada.')

    def query(self, sql, params=None, single=False, commit=False):
        cursor = None

        if not self.mydb or not self.mydb.is_connected():
            self.open()

        try:
            cursor = self.mydb.cursor()
            if params is None:
                self.cursor.execute(sql)
            else:
                if isinstance(params, list):
                    self.cursor.executemany(sql, params)
                else:
                    self.cursor.execute(sql, params)

            print("Consulta executada com sucesso.")

            if commit:
                self.commit()

            if single:
                return self.fetch_one()
            else:
                return self.fetch_all()

        except Error as e:
            print('Erro ao executar a consulta:', e, sql)

        finally:
            self.close()

    def execute(self, sql, params=None):
        try:
            self.open()

            if params is None:
                self.cursor.execute(sql)
            else:
                if isinstance(params, list):
                    self.cursor.executemany(sql, params)
                else:
                    self.cursor.execute(sql, params)

            print("Consulta executada com sucesso.")

        except Error as e:
            print('Erro ao executar a consulta:', e, sql)

        finally:
            self.close()

    def fetch_one(self):
        return self.cursor.fetchone()

    def fetch_all(self):
        return self.cursor.fetchall()

    def commit(self):
        try:
            self.mydb.commit()
            print("Commit executado com sucesso.")
        except Error as e:
            print("Erro ao executar o commit:", e)
            self.mydb.rollback()