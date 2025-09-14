# Registro de Erros e Soluções Durante o Projeto

Este documento lista os principais problemas enfrentados durante o desenvolvimento do projeto, bem como as soluções aplicadas para cada um.

---

## Problema 1: Perda de histórico de dados

O projeto estava **perdendo histórico**, ou seja, quando informações de clientes ou vendas estavam incompletas, **as vendas eram simplesmente excluídas**.

### Solução:

Foi necessário criar **camadas mais robustas**, e **persistir dados crus** em uma camada de *staging* (pré-processamento). O script `extract.py` teve papel fundamental nesse processo:

- Extrai os dados de um arquivo CSV contendo as vendas.
- Insere os dados em duas tabelas da camada **Raw**: `clientes_raw` e `vendas_raw`.
- Valida os registros com base na integridade mínima (e-mail e relacionamento cliente-venda).
- Retorna apenas os registros dos **últimos X dias** para processamento e transformação na camada **Cleansed**.

---

## Detalhes técnicos por tabela

### Tabela `clientes_raw`

- Criação da coluna `flag_valid` para identificar **clientes válidos** (com e-mail).
- `flag_valid = False` para e-mails **vazios**, **nulos** ou `"nan"`.
- Uso do comando `INSERT ... ON CONFLICT (email)` para **inserir ou atualizar dados** com base no campo de e-mail.

#### Possível erro:
> `não há nenhuma restrição de unicidade ou de exclusão que corresponda à especificação ON CONFLICT`

#### Correção:
Adição de uma **constraint de unicidade** no campo `email`:

`ALTER TABLE clientes_raw ADD CONSTRAINT unique_email UNIQUE (email);`

### Tabela `vendas_raw`

Para cada venda:
- Tenta buscar o id_cliente correspondente ao e-mail na tabela clientes_raw.
- Se encontrar, associa a venda ao id_cliente.
- Se não encontrar, o id_cliente será NULL e flag_valid = False.

#### Possível erro:
> `column "id_cliente" contains null values`

#### Correção:
Tornar o campo id_cliente opcional (nullable): `ALTER TABLE vendas_raw ALTER COLUMN id_cliente DROP NOT NULL;`

---

## Observação Importante
Em algumas situações, o script pode retornar um DataFrame vazio.  
Atenção ao intervalo de análise utilizado no filtro por data — pode estar fora do período com dados disponíveis.

---

## Resumo da Solução Aplicada
- Uso de flag_valid permite controle de qualidade dos dados já na camada Raw.
- Aceitação de NULL no campo id_cliente permite armazenar vendas mesmo sem correspondência com clientes válidos.
- Uso de CSV local simula uma pipeline real, com possibilidade de integração futura com APIs.