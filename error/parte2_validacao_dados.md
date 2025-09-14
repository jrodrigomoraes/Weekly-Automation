# 🐞 Registro de Erros e Soluções Durante o Projeto

## Parte 2 — Falhas com Great Expectations e abordagem alternativa com validação manual

---

### Contexto

Durante a tentativa de validar os dados extraídos, a primeira abordagem considerada foi o uso da biblioteca [Great Expectations (GE)](https://greatexpectations.io/), uma ferramenta bastante usada em projetos de validação de dados.

---

### Problemas enfrentados com Great Expectations

Apesar de sua popularidade, a integração do GE com o projeto não foi viável devido a diversos obstáculos:

#### 1. **Erros de versão e dependência**
- Dificuldade em instalar o GE corretamente no ambiente Dockerizado do Airflow.
- O executável CLI (`great_expectations`) **não era incluído no PATH**, impossibilitando execução via `subprocess` ou bash no container.
- Variações entre as versões do Python e as dependências do GE geraram conflitos.

#### 2. **Erros de execução e contextos inconsistentes**
Durante a execução no DAG do Airflow, o seguinte erro foi registrado:
`AttributeError: type object 'Validator' has no attribute 'from_pandas'`


Isso indicava que o contexto do GE estava mal configurado ou incorretamente inicializado e o objeto `Validator` não tinha o método esperado. Logs adicionais mostravam falhas ao localizar o contexto raiz do projeto e uso de `EphemeralDataContext` sem configuração.

Após diversas tentativas de correção e análise da documentação oficial, concluiu-se que o custo de integração **superava o benefício** imediato no cenário atual.

---

### Decisão: Substituir validação automatizada por uma função personalizada com `pandas`

Foi implementada uma função robusta de validação utilizando apenas `pandas`, com foco em:

- Validação de clientes e e-mails.
- Limpeza e padronização de nomes e estados.
- Mapeamento de status de pedidos para valores padronizados.
- Validação de vendas com base em data, valor e vínculo com clientes válidos.
- Log detalhado com alertas para casos inválidos.

---

## Implementação personalizada de validação

A função `validate_data()` realiza a separação dos dados em duas etapas:

### Validação de Clientes

- **Campos considerados:** `id_cliente_raw`, `nome`, `email`, `cidade`, `estado`.
- **Transformações:**
  - Normalização de e-mails (lowercase e remoção de espaços).
  - Nome e cidade padronizados (capitalização, preenchimento de nulos).
  - `estado` limitado aos dois primeiros caracteres.
- **Validações aplicadas:**
  - E-mail deve conter `@`, não pode ser vazio ou nulo.
  - Nome não pode estar em branco.

Clientes válidos são separados e renomeados para manter consistência (`id_cliente_raw → id_cliente`).

### Validação de Vendas

- **Campos considerados:** `id_venda_raw`, `id_cliente_raw`, `email`, `data_venda`, `valor_venda`, `status_pedido`.
- **Transformações:**
  - Mapeamento de status para valores padronizados:
    - Ex: `"concluído"`, `"finalizado"` → `"entregue"`.
  - Conversão da `data_venda` para datetime, com controle de parsing.
  - Checagem de valores de venda (`valor_venda >= 0`).
- **Validações aplicadas:**
  - A venda deve ter data válida, valor válido e cliente válido (presente nos clientes validados).

---

### Saídas geradas

- Os dados validados são salvos como `.pkl` em:
  - `/opt/airflow/temp/clientes_validos.pkl`
  - `/opt/airflow/temp/vendas_validas.pkl`

Essa etapa garante que somente dados consistentes seguem para a próxima camada da pipeline.

---

### Exemplo de log gerado

```
2025-09-09 06:16:45 - INFO - Iniciando validação dos dados...
2025-09-09 06:16:45 - WARNING - 3 clientes inválidos serão descartados:
2025-09-09 06:16:45 - WARNING - id_cliente_raw=999, nome='', email='nan'
2025-09-09 06:16:45 - INFO - 87 clientes válidos salvos em /opt/airflow/temp/clientes_validos.pkl
2025-09-09 06:16:45 - INFO - 195 vendas válidas salvas em /opt/airflow/temp/vendas_validas.pkl
```

---

## Conclusão e aprendizados
- Apesar da tentativa frustrada com Great Expectations, a solução alternativa entregou o mesmo resultado de forma mais controlável, leve e rastreável.
- O uso de logs e separação clara entre dados válidos/descartados aumentou a transparência da validação.
- O projeto ficou mais independente de bibliotecas externas complexas, o que é importante especialmente em ambientes com restrições como o Airflow via Docker.
