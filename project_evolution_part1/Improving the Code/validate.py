#!/usr/bin/env python
# coding: utf-8

# In[6]:


#Importante executar gx init se for a primeira vez

import great_expectations as gx
import logging
import os

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def validate_data(clientes, vendas):
    logger.info("Iniciando validação de dados...")

    #Cria (ou pega) o contexto do projeto GE
    context = gx.get_context()

    #Cria validadores a partir de DataFrames
    clientes_validator = context.sources.pandas_default.read_dataframe(clientes)
    vendas_validator = context.sources.pandas_default.read_dataframe(vendas)

    #Adiciona expectativas para clientes, nomes e validação de email
    clientes_validator.expect_column_values_to_not_be_null("nome")
    clientes_validator.expect_column_values_to_match_regex("email", r"[^@]+@[^@]+\.[^@]+")

    #Adiciona expectativas para vendas, também faz um check em id_cliente para validar a venda
    #Além disso, verificamos se a data está no formato correto
    vendas_validator.expect_column_values_to_not_be_null("id_cliente")
    vendas_validator.expect_column_values_to_be_between("valor_venda", min_value=0, max_value=100000)
    vendas_validator.expect_column_values_to_be_of_type("data_venda", "datetime64[ns]")

    logger.info("Expectativas definidas. Executando validação e gerando relatório...")

    #Salvando as Expectation Suites em disco para persistência e reuso
    #Isso gera arquivos JSON com as regras de validação dentro da pasta 'great_expectations/expectations/'
    #Importante para versionar, auditar e aplicar as mesmas regras em execuções futuras
    context.save_expectation_suite(
        clientes_validator.get_expectation_suite(), expectation_suite_name="clientes_suite"
    )
    context.save_expectation_suite(
        vendas_validator.get_expectation_suite(), expectation_suite_name="vendas_suite"
    )
    
    results = context.run_validation_operator(
        "action_list_operator",
        assets_to_validate=[clientes_validator, vendas_validator]
    )

    #Gera o relatório HTML (DataDocs)
    context.build_data_docs()

    logger.info("Validação concluída com sucesso. Relatório salvo em: great_expectations/uncommitted/data_docs/local_site/index.html")

    if not results["success"]:
        logger.error("Validação apresentou falhas!")

    return results["success"]


# In[ ]:




