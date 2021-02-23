from psycopg2.extras import execute_values
import psycopg2
import pandas as pd
import ConnectionDB
from typing import NamedTuple

class InfoReaderCsv(NamedTuple):
    Done: bool
    Frame: pd.DataFrame
    ErrorMessage: str

def csvReader(caminho: str, delimitador: str): 
    df = pd.DataFrame
    try:   
        df = pd.read_csv(caminho, sep=';',encoding="utf-8",decimal=",")
        df = df.where(pd.notnull(df), None)
        return InfoReaderCsv(True, df, '')
    except Exception as e:
        return InfoReaderCsv(False, df, e)
 
class InfoValidatorCsv(NamedTuple):
    Done: bool
    Frame: object
    ErrorMessage: str

def csvValidator(df):
    data = [] 
    for row in df.itertuples():
        try:
            data.append((row.CHAVE,row.NTIPODOC,row.NCHITEM,row.NCHDOC,row.NEMPRESA,row.VRQTD,row.TIPO,row.NPRODUTO,row.TCODIGO,row.TCODIGOBARRAS,row.TREFERENCIA,row.NVALOR,row.NQUANTIDADE,row.NQTDREAL,row.NTOTAL,row.NDESCONTO,row.NACRESCIMO,row.DATA,row.HORA,row.DCOMISSAO,row.NCLIENTE,row.TCLIENTE,row.NVENDEDOR,row.DESC_VENDEDOR,row.TTABELA,row.TUSUARIO,row.DESC_PRODUTOS,row.CONDICAOPGTO,row.NPAGAMENTO,row.TGRUPO,row.NGRUPO,row.NSUBGRUPO,row.TSUBGRUPO,row.NESTOQUE,row.DESC_ESTOQUE,row.NFORNECEDOR,row.DESC_FORNECEDOR,row.NDOCUMENTO,row.NSUPERVISOR,row.DESC_SUPERVISOR,row.NJUROS,row.NFRETE,row.NOUTRASDESPESAS,row.NVALORACRESCIMOINCLUSO,row.NAGENC_VALORUNIT,row.NAGENC_VALORTOTAL,row.NVENDEDOR2,row.DESC_VENDEDOR2,row.NVENDEDOR3,row.DESC_VENDEDOR3,row.NREF,row.NCHUNIDADE,row.TUNIDADE,row.NVALORLUCRO,row.NCUSTOITEM,row.CREATED_AT,row.UPDATED_AT))
        except Exception as e:
            return InfoValidatorCsv(False, data, e)
    return InfoValidatorCsv(True, data, '')

def csvInsert(conex,dados,id_queue):
    try:
        records_list_template = ','.join(['%s'] * len(dados))
        insert_query = 'insert into fat (chave, ntipodoc, nchitem, nchdoc, nempresa,vrqtd, tipo, nproduto,tcodigo, tcodigobarras,treferencia, nvalor, nquantidade,nqtdreal,ntotal,ndesconto,nacrescimo,ddata,hhora,dcomissao,ncliente,tcliente,nvendedor,desc_vendedor,ttabela,tusuario,desc_produtos,condicaopgto,npagamento,tgrupo,ngrupo,nsubgrupo,tsubgrupo,nestoque,desc_estoque,nfornecedor,desc_fornecedor,ndocumento,nsupervisor,desc_supervisor,njuros,nfrete,noutrasdespesas,nvaloracrescimoincluso,nagenc_valorunit,nagenc_valortotal,nvendedor2,desc_vendedor2,nvendedor3,desc_vendedor3,nref,nchunidade,tunidade,nvalorlucro,ncustoitem,created_at,updated_at) values {}'.format(records_list_template)
        con = conex.Cursor.cursor()
        con.execute(insert_query, dados)
        conex.Cursor.commit()
        csvResponse(conex, True, id_queue, 'Processado')
    except psycopg2.OperationalError as e:
        csvResponse(conex, False, id_queue, e)
        
def csvResponse(conex, status, id_queue, ms):
    con = conex.Cursor.cursor()
    print(status)
    if (status != True):
        con.execute('update file_queue set processed = true, erro = true, msg ='+'\''+ms+'\'')
        conex.Cursor.commit()
    else: 
        con.execute('update file_queue set processed = true, erro = false, msg ='+'\''+ms+'\'')
        conex.Cursor.commit()









    