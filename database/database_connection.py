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

    def query(self, sql, params=None, single=False, noret=False, commit=False):
        try:
            if not self.mydb or not self.mydb.is_connected():
                self.open()

            try:
                cursor = self.mydb.cursor()
                cursor.execute(sql, params)
                print("A consulta foi executada com sucesso.")
            except Exception as e:
                print("Erro ao executar a consulta:", e)


            if commit:
                try:
                    self.mydb.commit()
                    print("Commit executado com sucesso.")
                except Exception as e:
                    print("Erro ao executar o commit:", e)

            if noret:
                return None
            if single:
                return cursor.fetchone()
            else:
                return cursor.fetchall()

        except Error as e:
            print('Erro ao executar a consulta:', e, sql)

        finally:
            self.close()




