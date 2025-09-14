#!/usr/bin/env python
# coding: utf-8

# In[1]:

import great_expectations as gx
import logging
import pandas as pd

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def validate_data(clientes: pd.DataFrame, vendas: pd.DataFrame):
    logger.info("Iniciando validação de dados...")

    # Cria (ou pega) o contexto do projeto GE
    context = gx.get_context()

    # Cria validadores a partir de DataFrames
    clientes_validator = context.sources.pandas_default.read_dataframe(clientes)
    vendas_validator = context.sources.pandas_default.read_dataframe(vendas)

    #Definindo expectativas para clientes
    clientes_validator.expect_column_values_to_not_be_null("nome")
    clientes_validator.expect_column_values_to_match_regex("email", r"[^@]+@[^@]+\.[^@]+")

    #Definindo expectativas para vendas
    vendas_validator.expect_column_values_to_not_be_null("id_cliente_raw")
    vendas_validator.expect_column_values_to_be_between("valor_venda", min_value=0, max_value=100000)
    vendas_validator.expect_column_values_to_be_of_type("data_venda", "datetime64[ns]")

    logger.info("Expectativas definidas. Executando validação...")

    #Salva suites para reuso
    context.add_or_update_expectation_suite("clientes_suite", clientes_validator.get_expectation_suite())
    context.add_or_update_expectation_suite("vendas_suite", vendas_validator.get_expectation_suite())

    #Executa validações
    clientes_results = clientes_validator.validate()
    vendas_results = vendas_validator.validate()

    # Gera Data Docs (HTML de relatório)
    context.build_data_docs()

    logger.info(
        "Validação concluída. Relatório salvo em: great_expectations/uncommitted/data_docs/local_site/index.html"
    )

    #Se qualquer suite falhar, retorna DataFrames e False
    if not clientes_results.success or not vendas_results.success:
        logger.error("Validação apresentou falhas!")
        return clientes, vendas, False

    return clientes, vendas, True