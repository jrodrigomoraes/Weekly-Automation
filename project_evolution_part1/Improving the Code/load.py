#!/usr/bin/env python
# coding: utf-8

# In[2]:


import logging


# In[4]:


logger = logging.getLogger(__name__) #Ter um logger com o nome 'transform'
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_data(clientes, vendas):
    logger.info("Carregando dados em CSV (simulação do banco)...")
    
    clientes.to_csv("output/clientes.csv", index=False)
    vendas.to_csv("output/vendas.csv", index=False)
    
    logger.info("Dados carregados com sucesso em /output")
    
    return True


# In[ ]:




