import pandas as pd

# Lendo o DataSet
df = pd.read_csv('/home/lucassouza/vinha/Dashboard7/public/storage/CsvFiles/guedinho.csv', sep=';',decimal=",")
insert = []


for row in df.itertuples():
    insert.append((row.CHAVE,row.NTIPODOC,row.NCHITEM,row.NCHDOC,row.NEMPRESA,row.VRQTD,row.TIPO,row.NPRODUTO,row.TCODIGO,row.TCODIGOBARRAS,row.TREFERENCIA,row.NVALOR,row.NQUANTIDADE,row.NQTDREAL,row.NTOTAL,row.NDESCONTO,row.NACRESCIMO,row.DATA,row.HORA,row.DCOMISSAO,row.NCLIENTE,row.TCLIENTE,row.NVENDEDOR,row.DESC_VENDEDOR,row.TTABELA,row.TUSUARIO,row.DESC_PRODUTOS,row.CONDICAOPGTO,row.NPAGAMENTO,row.TGRUPO,row.NGRUPO,row.NSUBGRUPO,row.TSUBGRUPO,row.NESTOQUE,row.DESC_ESTOQUE,row.NFORNECEDOR,row.DESC_FORNECEDOR,row.NDOCUMENTO,row.NSUPERVISOR,row.DESC_SUPERVISOR,row.NJUROS,row.NFRETE,row.NOUTRASDESPESAS,row.NVALORACRESCIMOINCLUSO,row.NAGENC_VALORUNIT,row.NAGENC_VALORTOTAL,row.NVENDEDOR2,row.DESC_VENDEDOR2,row.NVENDEDOR3,row.DESC_VENDEDOR3,row.NREF,row.NCHUNIDADE,row.TUNIDADE,row.NVALORLUCRO,row.NCUSTOITEM,row.CREATED_AT,row.UPDATED_AT))
print(insert) 
# print(df.dtypes)   
# Identificando dados nullos
# print(df.isnull().sum())
# df['NTOTAL'].str[",","."].astype(float)
# df['NVALOR'].str[",","."].astype(float)
# df['NVALORLUCRO'].str[",","."].astype(float)
# df['NCUSTOITEM'].str[",","."].astype(float)


