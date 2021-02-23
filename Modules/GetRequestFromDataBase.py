import sys
import ConnectionDB
import RequestListFromDataBase
import Csvtools
import pandas as pd

def getRequestsFromDataBase(host: str, port: int, database: str, user: str, password: str):
    
    """ Primeiro passo -> Conectar no banco do cliente """
    aConnection = ConnectionDB.connectDataBase('pgsql',host, port, database, user, password)
    """ Segundo passo -> Encontrar os arquivos pendentes de processamento """
    LListaArquivosASeremProcessados = RequestListFromDataBase.processQueue(aConnection) 

    #Obter lista de arquivos a serem processados em cada banco da lista
    try:
        if len(LListaArquivosASeremProcessados) >= 0:
            for item in LListaArquivosASeremProcessados:
                df = (Csvtools.csvReader(item.caminho_arquivo, ';'))
                if df.Done != True:
                    print('Erro na leitura do csv')
                else:
                    lista = (Csvtools.csvValidator(df.Frame))
                    if lista.Done != True:
                        Csvtools.csvResponse(aConnection, False, item.id, 'Erro de validação do arquivo csv')
                    else:
                        Csvtools.csvInsert(aConnection,lista.Frame,item.id)       
    finally:
        aConnection.Cursor.close()