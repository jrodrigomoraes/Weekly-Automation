# Parte 3 — Problemas ao subir o Apache Airflow com Docker Compose

## Objetivo

Colocar o Apache Airflow online em um ambiente local utilizando Docker Compose, com interface acessível via navegador (`http://localhost:8080`).

---

## Problema Inicial

Ao tentar iniciar os serviços com `docker-compose`, o site `localhost:8080` **não carregava**, gerando dúvidas sobre se o ambiente estava funcionando corretamente.

Além disso, **erros relacionados ao banco de dados iniciando depois dos serviços do Airflow** também causaram falhas inesperadas.

---

## Etapas Executadas, Problemas e Soluções

### 1. Inicialização do banco de dados

**Comando executado:**

`docker-compose run airflow-webserver airflow db init`
**O que esse comando faz:**
- Inicializa o banco de dados de metadados do Airflow.
- Cria as tabelas no Postgres (ou backend configurado).

### Log Relevante
`DeprecationWarning: The sql_alchemy_conn option in [core] has been moved...`
Não é um erro, apenas um aviso de depreciação. A configuração sql_alchemy_conn foi movida de [core] para [database] no airflow.cfg. O banco foi inicializado corretamente.
**Resultado final:** `Initialization done`

---

## Subida dos Serviços
`docker-compose up -d`
**O que esse comando faz:**
- Sobe todos os serviços definidos no docker-compose.yml em background:
- postgres (banco de metadados)
- redis (broker para workers)
- airflow-webserver
- airflow-scheduler
- airflow-worker

### Log Relevante
`Found orphan containers...`
Contêineres "órfãos" são serviços que existiam de execuções anteriores e não estão mais referenciados no docker-compose.yml atual. Não é um erro, apenas um aviso.
Se quiser remover: `docker-compose up -d --remove-orphans`

### Resultado:
Todos os serviços foram iniciados sem erros críticos.

---

## Diagnósticos Via Logs
`docker-compose logs airflow-webserver`

### Trecho Relevante

```
Starting gunicorn...
Listening at: http://0.0.0.0:8080
...
GET /login ...
200 OK
```
- O Gunicorn, servidor WSGI do Airflow, iniciou corretamente.
- O servidor começou a escutar na porta 8080.
- Requisições HTTP estavam chegando e sendo processadas com sucesso (200 OK).

---

## Problema com o tempo de subida dos serviços

**Sintoma:**
- O navegador acessava localhost:8080 antes que o Webserver estivesse pronto, causando erros de conexão.

**Causa provável:**

O Airflow leva vários segundos para inicializar (principalmente Gunicorn + Webserver). Se o banco de dados (Postgres) não estiver pronto antes do Airflow tentar se conectar, isso pode gerar falhas de conexão.

**Solução:**
- Simplesmente aguardar o boot completo dos serviços (geralmente entre 15–30 segundos após up -d).
- Verifique com docker-compose ps e docker-compose logs se todos os containers estão "healthy".
- Em casos mais graves, use uma estratégia de restart com depends_on e restart: always no docker-compose.yml.

---

## Criação de usuário administrador
O Airflow estava com autenticação ativa (`/login`), portanto, foi necessário criar um usuário:

```
docker-compose run airflow-webserver airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com
```

**Resultado:**
- Usuário criado com sucesso.
- Interface de login passou a funcionar normalmente.

---

## Resumo Técnico e Comandos

| Etapa                        | Comando                                                | Descrição                                            |
| -----------------------------| ----------------------------------------------------   | ---------------------------------------------------- |
| Inicialização do banco       | `docker-compose run airflow-webserver airflow db init` | Cria as tabelas iniciais no banco do Airflow         |
| Subida dos serviços          | `docker-compose up -d`                                 | Inicia todos os serviços no modo background          |
| Limpeza de órfãos (opcional) | `docker-compose up -d --remove-orphans`                | Remove containers antigos não referenciados          |
| Ver logs                     | `docker-compose logs airflow-webserver`                | Inspeciona se o Webserver está escutando e saudável  |
| Criar usuário admin          | *comando acima*                                        | Cria credenciais para acessar a interface do Airflow |

---

## Outras Observações Técnicas e Conclusão

- O backend de autenticação padrão usado é o FAB (Flask AppBuilder).
- A configuração padrão exige login mesmo em ambiente local.
- O Airflow pode apresentar erros se o Postgres demorar a subir — para isso, considere configurar depends_on e healthcheck no docker-compose.yml.

## Conclusão
Apesar de algumas mensagens de warning e da latência inicial dos serviços, nenhum erro crítico foi encontrado. Os problemas se resumiram a:

- Timing do boot dos serviços
- Confusão com mensagens de depreciação
- Acesso à interface antes da inicialização completa
- Falta de usuário admin

Após resolver esses pontos, o Airflow ficou acessível via:
`http://localhost:8080`





