#!/usr/bin/env python
# coding: utf-8

# In[7]:


import pandas as pd
import logging


# In[13]:


#Configurando logging
logger = logging.getLogger(__name__) #Ter um logger com o nome 'extract'
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

"""
Vamos considerar 3 caminhos. O primeiro caminho que irei considerar é apenas para exemplo, visto que na realidade
de uma empresa, os caminhos 2 e 3, seriam os normalmemente seguidos.

1 - Inserindo dados "na mão"

2 - Inserindo atráves de um arquivo

3 - Inserindo através de uma API

Os caminhos 2 e 3, vou manter comentado.

"""

#Primeira Forma

def extract_data():
    logger.info("Iniciando extração de dados...")
    
    clientes = pd.DataFrame({
        "nome": ["Ana Flávia", "João Moreno da Silva", "Maria Doce", None],
        "email": ["ana@email.com", "joao@email.com", None, "maria@email"],
        "cidade": ["São Paulo", "Rio de Janeiro", "Belo Horizonte", "São Paulo"],
        "estado": ["SP", "RJ", "MG", None]
        })
    
    vendas = pd.DataFrame({
        "id_cliente": [29, 233, 108, 456],
        "data_venda": ["2025-08-01", "2025-08-02", "2025-08-03", None],
        "valor_venda": [200.0, 150.5, None, 300.0],
        "status_pedido": ["concluído", "pendente", "entregue", None]
        })
    
    logger.info("Extração concluída com sucesso.")
    
    return clientes, vendas


# In[11]:


#Segunda Forma

#clientes = pd.read_csv('clientes.csv')
#vendas = pdf.read_excel('vendas.xlsx')


# In[12]:


#Terceira forma

import requests

def extract_data():
    logger.info("Iniciando extração de dados...")
    #Interagindo com a API clientes
    response = requests.get("https://api.exemplo.com/clientes")
    
    clientes = pd.DataFrame(response.json())
    
    #Interagindo com a API vendas
    response = requests.get("https://api.exemplo.com/vendas")
    vendas = pd.DataFrame(response.json())
    
    logger.info("Extração de API concluída.")
    
    return clientes, vendas


# In[ ]:




