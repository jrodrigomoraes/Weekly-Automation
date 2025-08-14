# Projeto de Automação de Relatórios

## Objetivo
Automatizar o processo de atualização de dados no banco, gerar relatórios em CSV e PDF, e enviá-los automaticamente por e-mail para os destinatários definidos.

## Situação
Antes da automação, as etapas de coleta, análise e envio de relatórios eram realizadas manualmente, consumindo tempo e aumentando a chance de erros.  
Este projeto resolve esse problema integrando todas as etapas em um fluxo automatizado, podendo ser executado por um simples comando ou agendado. Além disso, pode ser executado durante mais vezes na semana, sem ocupar tempo extra dos colaboradores.

## Etapas do Projeto
1. **Atualização do Banco de Dados**  
   - Script para inserir e atualizar os dados na base SQL.
2. **Geração de CSVs**  
   - Exportação dos dados tratados para arquivos CSV.
3. **Criação de Relatórios PDF**  
   - Relatórios formatados com tabelas e gráficos prontos para apresentação.
4. **Envio Automático por E-mail**  
   - Envio dos PDFs e CSVs gerados para uma lista de e-mails pré-definida.
5. **Automação via Script .BAT**  
   - Execução sequencial dos scripts em ambiente Windows.


## Melhorias em andamento para robustez

- **Etapas de limpeza e transformação**
  - Padronização, tratamento de nulos, formatação de tipos, etc.

- **Validação com Great Expectations**
  - Garantia de qualidade dos dados antes de exportar ou usar nos relatórios.

- **Monitoramento de logs**
  - Uso do módulo `logging` para registrar eventos, erros, e status das etapas.

- **Orquestração com Airflow**
  - DAGs com tarefas separadas, controle de dependências, retries, e status dos jobs.

- **Armazenamento no Amazon S3**
  - Salvar CSVs e PDFs versionados em buckets com data/hora.

- **Monitoramento ativo (falhas e execuções)**
  - Notificações por e-mail ou Telegram em caso de falha
  - Dashboard de execuções com histórico de sucesso/falha (via Airflow UI ou logs exportados)
  - Registro de execuções em arquivos CSV ou base simples para controle e auditoria

---

## Configuração

### Instalar Dependências

`pip install -r requirements.txt`

### Configurar Variáveis de Ambiente
Criar uma arquivo .env com:

`EMAIL_USER=seu_email  
EMAIL_PASSWORD=sua senha  
SMTP_SERVER=smtp.seuprovedor.com  
SMTP_PORT=587`

### Execução de arquivo .bak (restauração de backup)

Este método foi usado originalmente para restaurar a base de dados a partir de um arquivo `.bak`.  
Apesar de ser funcional, é uma abordagem mais manual e menos portátil.

**Motivo de manter no repositório:**
- Registro histórico do processo original
- Útil para reprocessamentos ou ambientes controlados

Segue uma versão adaptável:
`
@echo off
REM =======================================================
REM Script para executar a automação do projeto
REM Adapte os caminhos abaixo conforme seu computador
REM =======================================================

REM ===== 1 - Caminho para ativar o ambiente virtual =====
call "CAMINHO_COMPLETO\venv\Scripts\activate"

REM ===== 2 - Ir para a pasta raiz do projeto =====
cd /d "CAMINHO_COMPLETO_DO_PROJETO"

REM ===== 3 - Executar scripts na ordem =====
python update_database.py
python analise_gera_csv.py
python gera_relatorio_pdf.py
python send_report.py

REM ===== 4 - Desativar ambiente virtual =====
deactivate

REM ===== 5 - Mensagem final =====
echo.
echo ===============================================
echo Relatório gerado e enviado com sucesso!
echo ===============================================
pause

`

**Atenção:** Este método será **substituído por um processo mais moderno**, orquestrado via **Apache Airflow**, com integração a fontes de dados automatizadas e controle de versionamento.

**Nova abordagem:** A DAG do Airflow será responsável por:
- Restaurar/atualizar a base a partir de fontes confiáveis
- Validar os dados com Great Expectations
- Orquestrar o fluxo completo com logging, monitoramento e retries

## Tecnologias Usadas

- Python 3
- Pandas – Tratamento e análise de dados
- Matplotlib – Visualização de dados
- Fpdf – Criação de PDFs
- smtplib – Envio de e-mails
- PostgreSQL / SQL Server – Banco de dados
- dotenv – Variáveis de ambiente
- Windows BAT Script – Automação

- **Essa stack será atualizada!**

## License

Este projeto está licenciado sob a licença MIT - veja o arquivo LICENSE para mais detalhes.
