# Parte 4 — Modelagem e Implementação de Histórico com SCD Tipo 2

## Objetivo

Implementar um modelo que preserve o histórico de alterações de status das vendas ao longo do tempo, utilizando a técnica de Slowly Changing Dimension Tipo 2 (SCD2), garantindo rastreabilidade e versionamento seguro no pipeline.

---

## O que é SCD Tipo 2?

SCD2 (Slowly Changing Dimension Tipo 2) é uma técnica de modelagem de dados usada para **preservar o histórico de alterações** em dados dimensionais.

Diferente de um `UPDATE` simples, que sobrescreve o dado antigo, o SCD2:

- **Fecha a versão anterior** com uma data de validade (`valid_to`)
- **Cria um novo registro** com os dados atualizados e uma nova data de início (`valid_from`)
- Marca qual é a **versão atual** com um booleano (`is_current`)

### Quando usar?

- Quando os dados **mudam com o tempo** e você precisa **auditar ou reconstruir versões passadas**.
- Ex: status de pedidos, endereços, cargos de funcionários, preços, etc.

---

## Validação de `id_venda_raw`

### Problema

Originalmente, o pipeline usava apenas um identificador local para as vendas (`id_venda`), gerado automaticamente no banco. Isso impossibilitava o controle de histórico, pois não havia forma de identificar se dois registros representavam a **mesma venda no sistema de origem**.

### Solução

Foi adicionado ao DataFrame e ao banco de dados o campo `id_venda_raw`, que representa a chave natural da venda na origem dos dados.

### Benefícios

- Permite **rastrear diferentes versões da mesma venda**
- Essencial para aplicar lógica de versionamento no `load.py`
- Evita duplicações indevidas no banco

---

## Alterações no `validate.py` para Suporte ao SCD2

### Motivo

O `validate_data()` original descartava algumas colunas brutas. Para aplicar SCD2 corretamente, era necessário **preservar `id_venda_raw` no DataFrame final** de vendas.

### Alterações Feitas

- A função `validate_data()` passou a retornar a coluna `id_venda_raw`
- Esse campo agora **é carregado até o final do pipeline**, chegando ao banco de dados
- Isso permite mapear o registro à sua versão original no sistema de origem

---

## Modelagem da Tabela `vendas` com SCD2

### Comandos SQL aplicados

```
ALTER TABLE vendas ADD COLUMN id_venda_raw TEXT;
ALTER TABLE vendas ADD COLUMN valid_from DATE NOT NULL DEFAULT CURRENT_DATE;
ALTER TABLE vendas ADD COLUMN valid_to DATE;
ALTER TABLE vendas ADD COLUMN is_current BOOLEAN DEFAULT true;
CREATE UNIQUE INDEX idx_venda_raw_current ON vendas(id_venda_raw) WHERE is_current = TRUE;
```

| Campo                   | Tipo    | Finalidade                                       |
| ----------------------- | ------- | ------------------------------------------------ |
| `id_venda_raw`          | TEXT    | Chave natural da venda                           |
| `valid_from`            | DATE    | Início da validade daquela versão                |
| `valid_to`              | DATE    | Fim da validade (NULL se versão ativa)           |
| `is_current`            | BOOLEAN | Marca se é a versão atual (TRUE)                 |
| `idx_venda_raw_current` | INDEX   | Garante que só existe uma versão ativa por venda |

## Resultado
Agora é possível manter várias versões da mesma venda, com histórico completo do campo status_pedido.

---

## Lógica do load.py para SCD2
**Problema**: O load.py original fazia apenas INSERT direto. Isso causava:
- Duplicações
- Sobrescrita de dados
- Perda de histórico

**Solução implementada**
A lógica foi reescrita com o seguinte fluxo:
- Para cada linha no DataFrame de vendas:
- Obtemos o id_venda_raw e status_pedido
- Verificamos se já existe uma versão ativa (is_current = TRUE) no banco:
- Se não existe, inserimos normalmente (primeira ocorrência dessa venda)
- Se existe e o status_pedido não mudou, ignoramos (não é uma nova versão)
- Se existe e o status_pedido mudou:

### Atualizamos a versão antiga:
` UPDATE vendas SET valid_to = hoje, is_current = FALSE WHERE id_venda_raw = ? AND is_current = TRUE`

### Inserimos nova versão:
`INSERT INTO vendas (...) VALUES (...) -- com valid_from = hoje, valid_to = NULL, is_current = TRUE`

### Técnicas usadas
- Controle de versão com valid_from, valid_to, is_current
- Upsert manual com SELECT + UPDATE + INSERT
- Cast explícito de tipos (vide próximo item)

**Erro Corrigido: Comparação entre TEXT e INTEGER (PostgreSQL)**
Problema: Ao executar a DAG com o novo campo id_venda_raw, o PostgreSQL retornou o erro:
`operator does not exist: text = integer`
Isso ocorre porque o banco espera comparar dois valores do mesmo tipo (TEXT = TEXT), mas o valor passado era int.

**Solução:** No load.py, foi feito cast explícito: `id_venda_raw = str(row.get('id_venda_raw')).strip()`
Assim, garantimos que a query SQL seja executada corretamente.

### Resumo de Boas Práticas e Impactos

| Tema               | Boas Práticas Aplicadas                                  |
| ------------------ | -------------------------------------------------------- |
| Modelagem de dados | Aplicação correta de SCD2                                |
| Validação          | Inclusão e validação do campo `id_venda_raw`             |
| Robustez           | Evita sobrescrita indesejada de registros                |
| Escalabilidade     | Permite evolução futura sem perda de dados antigos       |
| Segurança          | Cast explícito de tipos e controle por `is_current`      |
| Auditabilidade     | Histórico completo de status de pedidos mantido no banco |

---

## Conclusão
A introdução do SCD Tipo 2 foi essencial para garantir que o pipeline de vendas:

- Não perca o histórico de status
- Respeite as atualizações incrementais de forma segura
- Seja auditável e confiável para análises de longo prazo



