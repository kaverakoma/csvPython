import psycopg2
import ConnectionDB
from typing import NamedTuple

""" Estrutura de retorno dos arquivos que estão aguardando processamento """
class InfoArquivosAguardandoProcessamento(NamedTuple):
    id: int
    caminho_arquivo: str

""" Responsabilidade: Para determinado banco de dados, buscar a lista de arquivos que estão aguardando processamento """
def processQueue(AConnection):
    try:
        #conn = aconnex.connectGroup(host,port,stage,user,password)
        LCursor = ConnectionDB.selectDataBase(AConnection, 'select id, path from file_queue where processed = false')        
        rows = LCursor.fetchall()
        if LCursor.rowcount <= 0:
            return []        
        else:
            LListaRetorno = []
            for row in rows:
                LListaRetorno.append(InfoArquivosAguardandoProcessamento(row[0], row[1]))
            return LListaRetorno            
    except ValueError:
        print("Erro ao connectar ao banco")


