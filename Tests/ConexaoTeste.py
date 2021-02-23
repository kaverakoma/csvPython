import sys
sys.path.insert(0,'/home/lucassouza/vinha/Python_Queue/Models')
sys.path.insert(1,'/home/lucassouza/vinha/Python_Queue/Modules')
import unittest
# import ConexaoModel
import Conexao

class ConexaoTeste(unittest.TestCase):

    def teste_conexao_database(self):
        self.assertTrue(Conexao.connectDataBase('pgsql','127.0.0.1','5432', 'vinha7', 'postgres', 'root').Connected, 'Falha na conexão')

    # def teste_conexao_database_error(self):
    #     self.assertTrue(Conexao.connectDataBase().Connected, 'Falha na conexão')

if __name__ == "__main__":
    unittest.main()