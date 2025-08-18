#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import logging


# In[3]:


logger = logging.getLogger(__name__) #Ter um logger com o nome 'transform'
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def transform_data(clientes, vendas):
    logger.info("Iniciando transformação de dados...")
    
    #Vamos remover espaço em branco, garantir que é str e capitalizar as iniciais
    clientes['nome'] = clientes['nome'].fillna('Nome Desconhecido').astype(str).str.strip().str.title()
    
    #Tratando valores ausentes
    clientes['email'] = clientes['email'].fillna('sem_email@dominio.com') #Adiante vamos tratar melhor com great expectations
    clientes["estado"] = clientes["estado"].fillna("desconhecido")
    vendas["status_pedido"] = vendas["status_pedido"].fillna("desconhecido")
    
    #Para não distorcer KPI's, criamos uma nova coluna para valores 'desconhecidos' em venda.
    vendas["venda_incompleta"] = vendas["valor_venda"].isnull()
    
    #Validando data
    vendas['data_venda'] = pd.to_datetime(vendas['data_venda'], errors='coerce')
    
    #Criar flag para datas inválidas ou nulas (se fizer sentido para seu negócio)
    vendas['data_venda_invalida'] = vendas['data_venda'].isna()
    
    #Validando os valores de status_pedido
    status_map = {
        "concluído": "entregue",
        "finalizado": "entregue",
        "pendente": "em transporte",
        "em trânsito": "em transporte",
        "encaminhado": "em transporte",
        "atrasado": "atrasado"
    }
    
    vendas['status_pedido'] = vendas['status_pedido'].str.strip().str.lower().map(status_map).fillna('desconhecido')
    
    logger.info("Transformação concluída com sucesso.")
    
    return clientes, vendas


# In[ ]:




