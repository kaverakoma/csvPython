import sys
sys.path.insert(0,'/home/lucassouza/vinha/Python_Queue/Modules')
import ConnectionDB
import DataBasesList
import RequestListFromDataBase
import GetRequestFromDataBase
import Csvtools

#Obeter a conexão do banco da Vinhasoft
aConnection = ConnectionDB.connectDataBase('pgsql','127.0.0.1','5432', 'vinha7', 'postgres', 'root')

#Utilizar a conexão para fazer o select de grupos de empresas
ReturnDataBasesList = DataBasesList.getDashDatabasesList(aConnection)

#Utiliza o retorno das empresas para pegar as requests pendentes em cada banco
if len(ReturnDataBasesList) > 0:
    
        for LInfoBanco in ReturnDataBasesList:
            LListaArquivosASeremProcessados = GetRequestFromDataBase.getRequestsFromDataBase(LInfoBanco.host, LInfoBanco.porta, LInfoBanco.caminho_banco, LInfoBanco.usuario, LInfoBanco.senha)     
                  