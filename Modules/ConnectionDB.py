import psycopg2
from typing import NamedTuple

class InfoStatusConnection(NamedTuple):
    Connected: bool
    Cursor: object
    ErrorMessage: str

def connectDataBase(drive,host,port,database,user,password):
    connex = object
    try:
        connex = psycopg2.connect(host=host, port=port, database=database, user=user, password=password)
        return InfoStatusConnection(True, connex, '')
    except psycopg2.Error as erro:
        return InfoStatusConnection(False, connex, erro)

def selectDataBase(connection, sql):
    try:
        con = connection.Cursor.cursor()
        con.execute(sql)
        return con
    except ValueError:
        print("Erro ao connectar ao banco da vinha")

