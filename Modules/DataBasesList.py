from collections import deque
import RequestListFromDataBase
from typing import NamedTuple
import ConnectionDB

""" Estrutura dos itens de retorno das conexoes """
class InfoBanco(NamedTuple):
    host: str
    porta: int
    caminho_banco: str
    usuario: str
    senha: str

"""[summary]
    Responsabilidade: Buscar os bancos de dados existentes cadastrados no banco de dados da Vinhasoft
"""
def getDashDatabasesList(aConnection):
    SQL = 'SELECT * FROM GROUP_COMPANIES WHERE id <> 1 and active = true'    
    cursor = ConnectionDB.selectDataBase(aConnection, SQL)
    rows = cursor.fetchall()
    if cursor.rowcount <= 0:
        return []
    else:
        LListaRetorno = []
        for row in rows:
            LListaRetorno.append(InfoBanco(row[3], row[4], row[5], row[6], row[7]))
        return LListaRetorno