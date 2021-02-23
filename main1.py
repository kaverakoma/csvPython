import psycopg2
import connex
import process
import csvtools
import filas
import csv

def ProcessarArquivosPendentesBancoCliente(host: str, port: int, database: str, user: str, password: str):
    """ Primeiro passo -> Conectar no banco do cliente """
    aConnection = connex.connectGroup(host, port, database, user, password)
        
    """ Segundo passo -> Encontrar os arquivos pendentes de processamento """
    LListaArquivosASeremProcessados = process.processQueue(aConnection)
    
    try:
        if len(LListaArquivosASeremProcessados) >= 0:
            for item in LListaArquivosASeremProcessados:
                dados = csvtools.csvReader(item.caminho_arquivo, ',')
                csvtools.csvInsert(aConnection, dados)  
    finally:
        aConnection.close()
  





""" Primeiro passo -> conectar no banco de dados da Vinhasoft """
aConnection = connex.connect()
try:
    """ Segundo passo -> obter a lista de banco de dados dos clientes do dashboard """
    LListaRetornoListaInfoBancos = filas.getDashDatabasesList(aConnection)

    """ Terceiro passo -> Para cada banco de dados do cliente, processa os arquivos pendentes, se existirem """
    if len(LListaRetornoListaInfoBancos) > 0:
        for LInfoBanco in LListaRetornoListaInfoBancos:
            ProcessarArquivosPendentesBancoCliente(LInfoBanco.host, LInfoBanco.porta, LInfoBanco.caminho_banco, LInfoBanco.usuario, LInfoBanco.senha)           
finally:
    aConnection.close()    
