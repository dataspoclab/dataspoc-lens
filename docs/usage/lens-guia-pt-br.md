# DataSpoc Lens -- Guia Completo de Uso

## Sumario

1. [Introducao](#1-introducao)
2. [Instalacao](#2-instalacao)
3. [Quickstart](#3-quickstart)
4. [Configuracao](#4-configuracao)
5. [Comandos](#5-comandos)
6. [Shell Interativo](#6-shell-interativo)
7. [Transforms SQL](#7-transforms-sql)
8. [AI / Perguntas em Portugues](#8-ai--perguntas-em-portugues)
9. [Notebook (Jupyter)](#9-notebook-jupyter)
   - [Marimo (Notebook Reativo)](#marimo-notebook-reativo)
10. [Multi-cloud](#10-multi-cloud)
11. [Integracao com DataSpoc Pipe](#11-integracao-com-dataspoc-pipe)
12. [Export](#12-export)
13. [Cache Local](#13-cache-local)
14. [Troubleshooting](#14-troubleshooting)
15. [Exemplos Praticos](#15-exemplos-praticos)

---

## 1. Introducao

### O que e o DataSpoc Lens?

O DataSpoc Lens e um **virtual warehouse** que permite consultar arquivos Parquet armazenados na nuvem (S3, GCS, Azure) ou localmente usando SQL, sem precisar copiar dados ou manter infraestrutura de banco de dados.

Ele funciona como uma camada de consulta sobre o DuckDB, transformando diretorios de arquivos Parquet em tabelas virtuais (views) que podem ser consultadas de forma interativa, via CLI ou ate mesmo com perguntas em linguagem natural.

### Para quem e?

- **Engenheiros de dados** que precisam explorar dados gerados por pipelines (como o DataSpoc Pipe).
- **Analistas** que querem consultar dados no data lake sem configurar Spark, Athena ou BigQuery.
- **Cientistas de dados** que desejam explorar dados rapidamente via SQL ou Jupyter antes de iniciar modelagem.
- **Times pequenos** que precisam de um warehouse leve, sem custo de infraestrutura.

### O que resolve?

- Acesso SQL direto a arquivos Parquet na nuvem sem ETL adicional.
- Descoberta automatica de tabelas (via manifest ou scan de diretorios).
- Shell interativo com autocomplete e syntax highlighting.
- Perguntas em linguagem natural traduzidas para SQL via LLM (Ollama local, Claude ou GPT).
- Integracao nativa com Jupyter para analise exploratoria.
- Export de resultados em CSV, JSON e Parquet.

### Onde o Lens se encaixa na plataforma DataSpoc?

```
DataSpoc Pipe  --->  Bucket (Parquet)  --->  DataSpoc Lens  --->  Analise / ML
  (ingestao)           (storage)             (consulta SQL)
```

O **Pipe** ingere dados de APIs e os grava como Parquet no bucket. O **Lens** le esses dados e os disponibiliza como tabelas SQL. O fluxo e complementar: o Pipe escreve, o Lens le.

---

## 2. Instalacao

### Requisitos

- Python 3.10 ou superior
- pip (gerenciador de pacotes Python)
- Credenciais configuradas para o provedor de nuvem (se for acessar buckets remotos)

### Instalacao basica

```bash
pip install dataspoc-lens
```

### Extras opcionais

Dependendo do seu caso de uso, instale os extras necessarios:

```bash
# Suporte a Amazon S3
pip install dataspoc-lens[s3]

# Suporte a Google Cloud Storage
pip install dataspoc-lens[gcs]

# Suporte a Azure Blob Storage
pip install dataspoc-lens[azure]

# Integracao com JupyterLab
pip install dataspoc-lens[jupyter]

# Notebook reativo Marimo
pip install dataspoc-lens[marimo]

# Perguntas em linguagem natural via IA (Ollama local ou Anthropic/OpenAI na nuvem)
pip install dataspoc-lens[ai]

# Tudo de uma vez
pip install dataspoc-lens[all]
```

### Verificando a instalacao

```bash
dataspoc-lens --version
```

---

## 3. Quickstart

Passo a passo para ir do zero a primeira consulta em poucos minutos.

### Passo 1: Inicializar a configuracao

```bash
dataspoc-lens init
```

Isso cria o diretorio `~/.dataspoc-lens/` com o arquivo `config.yaml` e a pasta `transforms/`.

### Passo 2: Registrar um bucket

Se voce tem dados gerados pelo DataSpoc Pipe (ou qualquer diretorio com arquivos Parquet):

```bash
# Bucket S3
dataspoc-lens add-bucket s3://meu-data-lake

# Diretorio local
dataspoc-lens add-bucket file:///home/usuario/dados
```

O Lens vai descobrir automaticamente as tabelas no bucket e exibir um resumo.

### Passo 3: Ver o catalogo de tabelas

```bash
dataspoc-lens catalog
```

Saida esperada:

```
        Catalog
┏━━━━━━━━━━━┳━━━━━━━━━┳━━━━━━┳━━━━━━━━━┓
┃ Table     ┃ Columns ┃ Rows ┃ Source  ┃
┡━━━━━━━━━━━╇━━━━━━━━━╇━━━━━━╇━━━━━━━━━┩
│ users     │ 5       │ 1200 │ manifest│
│ orders    │ 8       │ 5430 │ manifest│
│ products  │ 6       │ 320  │ scan   │
└───────────┴─────────┴──────┴─────────┘
```

### Passo 4: Abrir o shell interativo

```bash
dataspoc-lens shell
```

```
DataSpoc Lens Shell
Type SQL or .help for commands. Ctrl+D or .quit to exit.

lens> SELECT COUNT(*) FROM orders;
+----------+
| count(*) |
+----------+
| 5430     |
+----------+
(1 row(s), 0.234s)
```

### Passo 5: Fazer perguntas em linguagem natural

```bash
# Com Ollama (padrao, local, sem chave de API)
dataspoc-lens setup-ai
dataspoc-lens ask "quantos pedidos tivemos ontem?"

# Ou com um provedor na nuvem
export DATASPOC_LLM_API_KEY=sk-...
dataspoc-lens ask "quantos pedidos tivemos ontem?"
```

```
SQL: SELECT COUNT(*) FROM orders WHERE order_date = CURRENT_DATE - INTERVAL 1 DAY

+----------+
| count(*) |
+----------+
| 42       |
+----------+
(1 linha(s), 0.512s)
```

---

## 4. Configuracao

### Estrutura de diretorios

```
~/.dataspoc-lens/
  config.yaml          # Configuracao principal (buckets registrados)
  transforms/          # Arquivos .sql de transformacao
  cache/               # Cache local de dados Parquet (ver secao 13)
  history              # Historico de comandos do shell
```

### Formato do config.yaml

```yaml
buckets:
  - s3://meu-data-lake
  - s3://outro-bucket/dados
  - file:///home/usuario/dados-locais
```

O arquivo e gerenciado automaticamente pelo comando `add-bucket`, mas pode ser editado manualmente.

### Configuracao do LLM (para o comando `ask`)

A configuracao do LLM fica em `~/.dataspoc-lens/config.yaml` e pode ser sobrescrita por variaveis de ambiente:

```yaml
llm:
  provider: ollama
  model: duckdb-nsql:7b    # ou qwen2.5-coder:1.5b para mais leve
```

| Variavel                 | Descricao                                    | Padrao           |
|--------------------------|----------------------------------------------|------------------|
| `DATASPOC_LLM_PROVIDER`  | Provedor: `ollama`, `anthropic` ou `openai`  | `ollama`         |
| `DATASPOC_LLM_MODEL`     | Nome do modelo                               | `duckdb-nsql:7b` |
| `DATASPOC_LLM_API_KEY`   | Chave de API (apenas para provedores cloud)  | (nenhum)         |

Variaveis de ambiente sobrescrevem os valores do config.yaml.

Exemplo de configuracao com provedor cloud:

```bash
# Para usar Anthropic Claude
export DATASPOC_LLM_PROVIDER=anthropic
export DATASPOC_LLM_API_KEY=sk-ant-...

# Para usar OpenAI GPT
export DATASPOC_LLM_PROVIDER=openai
export DATASPOC_LLM_API_KEY=sk-...
```

Para Ollama (padrao), nenhuma variavel de ambiente e necessaria. Basta executar `dataspoc-lens setup-ai`.

---

## 5. Comandos

### 5.1 init

Inicializa a configuracao do DataSpoc Lens.

```bash
dataspoc-lens init
```

- Cria o diretorio `~/.dataspoc-lens/`.
- Cria o arquivo `config.yaml` vazio.
- Cria o diretorio `transforms/`.

Se ja estiver inicializado, exibe a mensagem "Already initialized".

### 5.2 add-bucket

Registra um bucket e descobre as tabelas contidas nele.

```bash
dataspoc-lens add-bucket <URI>
```

**Exemplos:**

```bash
# Amazon S3
dataspoc-lens add-bucket s3://meu-bucket-dados

# Google Cloud Storage
dataspoc-lens add-bucket gs://meu-bucket-gcs

# Azure Blob Storage
dataspoc-lens add-bucket az://meu-container

# Diretorio local
dataspoc-lens add-bucket file:///dados/parquet
```

O comando faz duas coisas:
1. Salva a URI no `config.yaml`.
2. Executa a descoberta de tabelas (manifest ou scan) e exibe o resultado.

Se o bucket ja estiver registrado, ele pula o registro mas ainda executa a descoberta.

### 5.3 catalog

Lista todas as tabelas descobertas em todos os buckets registrados.

```bash
# Listar todas as tabelas
dataspoc-lens catalog

# Ver schema detalhado de uma tabela especifica
dataspoc-lens catalog --detail users
```

**Exemplo de saida com --detail:**

```
      Schema: users
┏━━━━━━━━━━━━━━┳━━━━━━━━━━┓
┃ Column       ┃ Type     ┃
┡━━━━━━━━━━━━━━╇━━━━━━━━━━┩
│ id           │ INTEGER  │
│ name         │ VARCHAR  │
│ email        │ VARCHAR  │
│ created_at   │ TIMESTAMP│
│ active       │ BOOLEAN  │
└──────────────┴──────────┘
```

### 5.4 query

Executa uma consulta SQL diretamente pela linha de comando.

```bash
dataspoc-lens query "SELECT * FROM orders LIMIT 10"
dataspoc-lens query "SELECT status, COUNT(*) FROM orders GROUP BY status"
```

As tabelas de todos os buckets registrados sao automaticamente montadas como views antes da execucao.

| Parametro        | Descricao                                                                 |
|------------------|---------------------------------------------------------------------------|
| `--export`, `-e` | Exporta resultados para arquivo. Formato detectado pela extensao (`.csv`, `.json`, `.parquet`) |

```bash
dataspoc-lens query "SELECT * FROM orders" --export resultado.csv
dataspoc-lens query "SELECT * FROM orders" -e dados.json
dataspoc-lens query "SELECT * FROM orders" -e dados.parquet
```

### 5.5 shell

Abre o shell interativo SQL com autocomplete e syntax highlighting. Veja a secao [Shell Interativo](#6-shell-interativo) para detalhes completos.

```bash
dataspoc-lens shell
```

#### Dot commands no shell

| Comando                    | Descricao                                   |
|----------------------------|---------------------------------------------|
| `.tables`                  | Lista todas as tabelas montadas             |
| `.schema <nome>`           | Mostra o schema de uma tabela               |
| `.buckets`                 | Lista os buckets registrados                |
| `.export <fmt> <caminho>`  | Exporta o ultimo resultado (csv/json/parquet)|
| `.cache <tabela>`          | Cacheia uma tabela localmente               |
| `.help`                    | Mostra ajuda dos comandos                   |
| `.quit` / `.exit`          | Sai do shell                                |

### 5.6 setup-ai

Configura o provedor de IA para perguntas em linguagem natural. Instala o Ollama (se nao estiver presente) e baixa o modelo padrao (`duckdb-nsql:7b`).

```bash
dataspoc-lens setup-ai
```

### 5.7 transform run / transform list

Gerencia e executa transforms SQL (veja secao [Transforms SQL](#7-transforms-sql)).

```bash
# Listar transforms disponiveis
dataspoc-lens transform list

# Executar todos os transforms em ordem
dataspoc-lens transform run
```

**Saida do transform run:**

```
Executando 001_clean_users.sql... OK (0.3s)
Executando 002_aggregate_orders.sql... OK (1.2s)
Executando 003_build_summary.sql... OK (0.5s)

3 transform(s) executado(s) com sucesso.
```

Se um transform falhar, a execucao para imediatamente e o erro e exibido.

### 5.8 notebook

Abre o JupyterLab com todas as tabelas pre-montadas. Requer o extra `[jupyter]`.

```bash
dataspoc-lens notebook
```

Use `--marimo` para abrir o Marimo em vez do JupyterLab (requer o extra `[marimo]`):

```bash
dataspoc-lens notebook --marimo
```

Veja a secao [Notebook (Jupyter)](#9-notebook-jupyter) para detalhes.

### 5.9 ask

Faz uma pergunta em linguagem natural e obtem resultados via SQL gerado por IA.

```bash
# Pergunta simples
dataspoc-lens ask "quais sao os 10 clientes que mais compraram?"

# Com modo debug (mostra o prompt enviado ao LLM)
dataspoc-lens ask --debug "qual o ticket medio por mes?"
```

```bash
dataspoc-lens ask "pedidos por cidade" --export cidades.csv
```

**Parametros:**

| Parametro        | Descricao                                         |
|------------------|----------------------------------------------------|
| `<pergunta>`     | Pergunta em linguagem natural (obrigatorio)        |
| `--debug`        | Exibe o prompt completo enviado ao LLM             |
| `--export`, `-e` | Exporta resultados para arquivo. Formato detectado pela extensao (`.csv`, `.json`, `.parquet`) |

Requer Ollama configurado (`dataspoc-lens setup-ai`) ou `DATASPOC_LLM_API_KEY` para provedores cloud. Veja secao [AI / Perguntas em Portugues](#8-ai--perguntas-em-portugues).

### 5.10 ml activate / ml status

Comandos relacionados ao DataSpoc ML (produto comercial).

```bash
# Ver informacoes sobre o DataSpoc ML
dataspoc-lens ml activate

# Verificar status do gateway ML
dataspoc-lens ml status
```

O DataSpoc ML e um produto comercial separado que permite treinar modelos, servir previsoes via API REST e monitorar drift. Para mais informacoes: https://dataspoc.com/ml

### 5.11 cache

Gerencia o cache local de dados Parquet remotos. Veja a secao [Cache Local](#13-cache-local) para detalhes completos.

```bash
# Cachear uma tabela localmente
dataspoc-lens cache orders

# Listar tabelas cacheadas (data, tamanho, status fresh/stale)
dataspoc-lens cache --list

# Forcar re-download de uma tabela
dataspoc-lens cache orders --refresh

# Limpar cache de uma tabela especifica
dataspoc-lens cache orders --clear

# Limpar todo o cache
dataspoc-lens cache --clear
```

**Parametros:**

| Parametro    | Descricao                                                |
|--------------|----------------------------------------------------------|
| `<tabela>`   | Nome da tabela a ser cacheada (opcional com --list/--clear) |
| `--list`     | Lista todas as tabelas cacheadas com status              |
| `--refresh`  | Forca re-download mesmo se ja estiver cacheada           |
| `--clear`    | Limpa o cache (de uma tabela ou de todas)                |

---

## 6. Shell Interativo

O shell interativo e a forma mais poderosa de explorar seus dados com o Lens.

### Iniciando o shell

```bash
dataspoc-lens shell
```

```
DataSpoc Lens Shell
Type SQL or .help for commands. Ctrl+D or .quit to exit.

lens>
```

### Recursos do shell

#### Autocomplete

O shell oferece autocomplete inteligente que inclui:
- Palavras-chave SQL (SELECT, FROM, WHERE, JOIN, GROUP BY, etc.).
- Nomes de tabelas descobertas nos buckets.
- Nomes de colunas de todas as tabelas.

Pressione `Tab` para ativar o autocomplete.

#### Syntax highlighting

As queries SQL sao coloridas automaticamente usando Pygments, facilitando a leitura e edicao.

#### Historico de comandos

Todos os comandos digitados sao salvos em `~/.dataspoc-lens/history`. Use as setas para cima/baixo para navegar pelo historico. O shell tambem oferece sugestao automatica baseada no historico (AutoSuggestFromHistory).

#### Auto-sugestao

Ao digitar, o shell sugere comandos anteriores semelhantes em cinza. Pressione a seta para a direita para aceitar a sugestao.

### Dot commands

Os dot commands sao comandos especiais que comecam com ponto (`.`):

#### .tables

Lista todas as tabelas montadas no DuckDB:

```
lens> .tables
  users (VIEW)
  orders (VIEW)
  products (VIEW)
```

#### .schema

Mostra o schema (colunas e tipos) de uma tabela:

```
lens> .schema orders
Table: orders
  id  INTEGER
  user_id  INTEGER
  product_id  INTEGER
  quantity  INTEGER
  total  DOUBLE
  status  VARCHAR
  order_date  DATE
  created_at  TIMESTAMP
```

#### .buckets

Lista os buckets registrados:

```
lens> .buckets
  s3://meu-data-lake
  file:///dados/locais
```

#### .export

Exporta o resultado da ultima query executada:

```
lens> SELECT * FROM users WHERE active = true;
(... resultado ...)

lens> .export csv /tmp/usuarios_ativos.csv
Exported 150 rows to /tmp/usuarios_ativos.csv (csv)

lens> .export json /tmp/usuarios_ativos.json
Exported 150 rows to /tmp/usuarios_ativos.json (json)

lens> .export parquet /tmp/usuarios_ativos.parquet
Exported 150 rows to /tmp/usuarios_ativos.parquet (parquet)
```

**Importante:** voce precisa executar uma query antes de usar `.export`. Caso contrario, o shell exibira a mensagem "No previous query result to export."

#### .help

Exibe a lista de dot commands disponiveis.

#### .quit / .exit

Sai do shell. Voce tambem pode usar `Ctrl+D`.

### Comando ask no shell

Dentro do shell, voce pode fazer perguntas em linguagem natural sem aspas:

```
lens> ask quantos pedidos foram feitos na ultima semana?
SQL: SELECT COUNT(*) FROM orders WHERE order_date >= CURRENT_DATE - INTERVAL 7 DAY

+----------+
| count(*) |
+----------+
| 287      |
+----------+
(1 linha(s), 0.623s)
```

O resultado da query gerada por IA tambem fica disponivel para `.export`.

---

## 7. Transforms SQL (Camada Gold)

Transforms sao arquivos SQL numerados que permitem ao analista construir a **camada gold** — agregacoes, metricas, reports — a partir dos dados curated que o Data Engineer preparou com o Pipe.

```
Pipe (DE):       source -> raw -> transform(df) -> curated    (Python, automatizado, cron)
Lens (Analista): curated -> transform SQL -> gold              (SQL, sob demanda, exploratorio)
```

| Camada | Quem | Ferramenta | Path | Proposito |
|--------|------|-----------|------|-----------|
| **Raw** | DE | Pipe | raw/ | Dados brutos da fonte |
| **Curated** | DE | Pipe transform | curated/ | Limpo, tipado, deduplicado |
| **Gold** | Analista | Lens transform | gold/ | Agregacoes, metricas, reports |

### Estrutura de arquivos

Os transforms ficam em `~/.dataspoc-lens/transforms/` e devem seguir a convencao de numeracao:

```
~/.dataspoc-lens/transforms/
  001_clean_users.sql
  002_aggregate_orders.sql
  003_build_summary.sql
```

A numeracao garante a ordem de execucao. Arquivos sao ordenados pelo prefixo numerico.

### Padrao CTAS (CREATE TABLE AS SELECT)

Os transforms tipicamente usam o padrao CTAS para criar novas tabelas a partir de queries:

**Exemplo: `001_clean_users.sql`**

```sql
CREATE OR REPLACE TABLE curated_users AS
SELECT
    id,
    TRIM(LOWER(name)) AS name,
    TRIM(LOWER(email)) AS email,
    created_at,
    active
FROM users
WHERE email IS NOT NULL
  AND email LIKE '%@%';
```

**Exemplo: `002_aggregate_orders.sql`**

```sql
CREATE OR REPLACE TABLE orders_monthly AS
SELECT
    DATE_TRUNC('month', order_date) AS month,
    COUNT(*) AS total_orders,
    SUM(total) AS revenue,
    AVG(total) AS avg_ticket,
    COUNT(DISTINCT user_id) AS unique_customers
FROM orders
WHERE status = 'completed'
GROUP BY 1
ORDER BY 1;
```

**Exemplo: `003_build_summary.sql`**

```sql
CREATE OR REPLACE TABLE executive_summary AS
SELECT
    u.name,
    COUNT(o.id) AS total_orders,
    SUM(o.total) AS total_spent,
    MAX(o.order_date) AS last_order
FROM curated_users u
LEFT JOIN orders o ON o.user_id = u.id
GROUP BY u.name
ORDER BY total_spent DESC;
```

### Convencao /curated/

Para transforms que geram saida persistente em bucket, recomenda-se gravar na pasta `curated/`:

```
s3://meu-bucket/
  raw/                 # Dados brutos do Pipe
    users/
    orders/
  curated/             # Dados processados pelos transforms
    clean_users/
      data.parquet
    orders_monthly/
      data.parquet
```

### Executando transforms

```bash
# Ver quais transforms estao disponiveis
dataspoc-lens transform list

# Executar todos em sequencia
dataspoc-lens transform run
```

A execucao e sequencial. Se um transform falhar, os subsequentes nao serao executados e o erro sera exibido.

---

## 8. AI / Perguntas em Portugues

O DataSpoc Lens permite fazer perguntas sobre seus dados em linguagem natural (inclusive em portugues). O provedor padrao e o **Ollama** (local, gratuito, sem chave de API). Provedores cloud (Anthropic Claude, OpenAI GPT) tambem sao suportados.

### Configuracao

#### Opcao A: Ollama (padrao -- local, gratuito)

```bash
dataspoc-lens setup-ai
```

Isso instala o Ollama (se nao estiver instalado) e baixa o modelo `duckdb-nsql:7b`. Nenhuma chave de API e necessaria.

#### Opcao B: Provedor cloud

```bash
pip install dataspoc-lens[ai]

# Para Anthropic
export DATASPOC_LLM_PROVIDER=anthropic
export DATASPOC_LLM_API_KEY=sk-ant-...

# Para OpenAI
export DATASPOC_LLM_PROVIDER=openai
export DATASPOC_LLM_API_KEY=sk-...
```

### Configuracao no config.yaml

As configuracoes de LLM ficam em `~/.dataspoc-lens/config.yaml`:

```yaml
llm:
  provider: ollama
  model: duckdb-nsql:7b    # ou qwen2.5-coder:1.5b para mais leve
```

Variaveis de ambiente sobrescrevem os valores do config:

| Variavel                 | Descricao                        | Padrao           |
|--------------------------|----------------------------------|------------------|
| `DATASPOC_LLM_PROVIDER`  | Provedor de LLM                 | `ollama`         |
| `DATASPOC_LLM_MODEL`     | Nome do modelo                  | `duckdb-nsql:7b` |
| `DATASPOC_LLM_API_KEY`   | Chave de API (provedores cloud) | (nenhum)         |

### Provedores suportados

| Provedor   | Modelo utilizado       | Chave de API necessaria |
|------------|------------------------|------------------------|
| Ollama (padrao) | duckdb-nsql:7b    | Nao (local)            |
| Anthropic  | Claude Sonnet          | Sim                    |
| OpenAI     | GPT-4o                 | Sim                    |

### Exemplos de perguntas

```bash
# Perguntas em portugues
dataspoc-lens ask "quantos usuarios ativos temos?"
dataspoc-lens ask "qual o produto mais vendido do ultimo mes?"
dataspoc-lens ask "quais clientes nao compraram nos ultimos 90 dias?"
dataspoc-lens ask "mostre a receita por semana dos ultimos 3 meses"

# Perguntas em ingles tambem funcionam
dataspoc-lens ask "show top 10 customers by revenue"
```

### Modo debug

O modo debug exibe o prompt completo enviado ao LLM, incluindo o DDL das tabelas e dados de exemplo:

```bash
dataspoc-lens ask --debug "qual o ticket medio?"
```

Saida:

```
--- Prompt enviado ao LLM ---
Voce e um assistente SQL. O banco e DuckDB.

Schema das tabelas disponiveis:

CREATE VIEW orders AS -- (id INTEGER, user_id INTEGER, total DOUBLE, ...)
Exemplo:
id | user_id | total | status | order_date
1 | 42 | 129.90 | completed | 2025-01-15
...

Pergunta do usuario: qual o ticket medio?

Responda APENAS com SQL valido para DuckDB. Sem explicacoes, sem markdown, apenas o SQL.
--- Fim do prompt ---

SQL: SELECT AVG(total) AS ticket_medio FROM orders WHERE status = 'completed'

+---------------+
| ticket_medio  |
+---------------+
| 87.45         |
+---------------+
(1 linha(s), 0.234s)
```

### Usando no shell interativo

Dentro do shell, use `ask` sem aspas:

```
lens> ask quais sao os 5 estados com mais pedidos?
```

### Como funciona internamente

1. O Lens coleta o DDL (schema) de todas as views montadas.
2. Busca 3 linhas de exemplo de cada tabela.
3. Monta um prompt com schema + exemplos + pergunta do usuario.
4. Envia ao LLM (Ollama, Claude ou GPT).
5. Extrai o SQL da resposta (suporta respostas com ou sem markdown fences).
6. Executa o SQL no DuckDB e exibe o resultado.

### Limitacoes

- O LLM pode gerar SQL incorreto. Nesses casos, tente reformular a pergunta de forma mais especifica.
- O contexto e limitado ao schema e a 3 linhas de exemplo por tabela. Dados com nomes de colunas ambiguos podem confundir o modelo.
- Perguntas muito complexas ou que envolvam logica de negocio nao explicita nos dados podem gerar resultados inesperados.
- Com provedores cloud, cada chamada consome tokens da sua API. Ollama roda localmente sem custo.
- A resposta do LLM e executada diretamente. Tenha cuidado em ambientes de producao.

---

## 9. Notebook (Jupyter)

O Lens pode abrir o JupyterLab com todas as tabelas ja montadas, pronto para analise exploratoria.

### Pre-requisitos

```bash
pip install dataspoc-lens[jupyter]
```

### Iniciando

```bash
dataspoc-lens notebook
```

Isso faz o seguinte:
1. Gera um script de startup em `~/.ipython/profile_default/startup/00-dataspoc-lens.py`.
2. O script cria uma conexao DuckDB, monta todas as views dos buckets registrados e carrega a extensao `jupysql`.
3. Abre o JupyterLab.

### Usando no notebook

Ao abrir um notebook, voce vera a mensagem:

```
DataSpoc Lens: 3 tabela(s) montada(s). Use %%sql para consultar.
```

#### Magic %%sql

O jupysql e carregado automaticamente, permitindo usar o magic `%%sql`:

```python
%%sql
SELECT * FROM orders LIMIT 10
```

```python
%%sql
SELECT status, COUNT(*) AS total
FROM orders
GROUP BY status
ORDER BY total DESC
```

#### Variavel `conn`

A conexao DuckDB tambem fica disponivel como a variavel `conn`, permitindo uso direto com Python:

```python
df = conn.execute("SELECT * FROM users LIMIT 100").fetchdf()
df.head()
```

### Marimo (Notebook Reativo)

O Marimo e um notebook reativo moderno -- as celulas se atualizam automaticamente quando as dependencias mudam. Mais interativo que o Jupyter para exploracao de dados.

```bash
# Instalar
pip install dataspoc-lens[marimo]

# Iniciar
dataspoc-lens notebook --marimo
```

Abre no navegador com as tabelas pre-montadas. Recursos:

- **Celulas reativas**: altere uma query e os resultados se atualizam automaticamente
- **Modo app**: alterne entre visualizacao de codigo e visualizacao limpa de app
- **Dataframes interativos**: filtre, ordene e explore com o mouse
- **Graficos integrados**: clique para gerar visualizacoes
- **Sem necessidade de "Run All"**: tudo reage as mudancas

O objeto `conn` esta disponivel em todas as celulas:

```python
conn.sql("SELECT * FROM orders LIMIT 10").df()
```

Use `dataspoc-lens notebook` para Jupyter (padrao) ou `--marimo` para Marimo.

---

## 10. Multi-cloud

O Lens suporta acesso a dados em multiplos provedores de nuvem simultaneamente. A conexao e feita via DuckDB httpfs e as credenciais sao gerenciadas pelo ambiente do sistema operacional.

### Amazon S3

**Instalacao:**

```bash
pip install dataspoc-lens[s3]
```

**Credenciais:**

Configure as credenciais AWS padrao:

```bash
export AWS_ACCESS_KEY_ID=AKIA...
export AWS_SECRET_ACCESS_KEY=...
export AWS_DEFAULT_REGION=us-east-1
```

Ou use o AWS CLI:

```bash
aws configure
```

**Registro do bucket:**

```bash
dataspoc-lens add-bucket s3://meu-bucket-s3/dados
```

### Google Cloud Storage

**Instalacao:**

```bash
pip install dataspoc-lens[gcs]
```

**Credenciais:**

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/caminho/para/service-account.json
```

Ou use o gcloud CLI:

```bash
gcloud auth application-default login
```

**Registro do bucket:**

```bash
dataspoc-lens add-bucket gs://meu-bucket-gcs/dados
```

### Azure Blob Storage

**Instalacao:**

```bash
pip install dataspoc-lens[azure]
```

**Credenciais:**

```bash
export AZURE_STORAGE_ACCOUNT=minhaaccount
export AZURE_STORAGE_KEY=...
```

**Registro do bucket:**

```bash
dataspoc-lens add-bucket az://meu-container/dados
```

### Local (sistema de arquivos)

Nenhum extra e necessario. Basta usar o esquema `file://`:

```bash
dataspoc-lens add-bucket file:///home/usuario/dados/parquet
```

### Multi-cloud simultaneo

Voce pode registrar buckets de provedores diferentes e consultar todos juntos:

```bash
dataspoc-lens add-bucket s3://dados-producao
dataspoc-lens add-bucket gs://dados-analytics
dataspoc-lens add-bucket file:///dados/locais

dataspoc-lens shell
```

```
lens> SELECT * FROM s3_users
      UNION ALL
      SELECT * FROM gcs_users;
```

---

## 11. Integracao com DataSpoc Pipe

O DataSpoc Lens foi projetado para trabalhar nativamente com os dados gerados pelo DataSpoc Pipe.

### Como o Pipe organiza os dados

O Pipe grava dados no formato Parquet em buckets, seguindo uma estrutura padrao:

```
s3://meu-bucket/
  .dataspoc/
    manifest.json       # Manifesto gerado pelo Pipe
  raw/
    users/
      part-0001.parquet
      part-0002.parquet
    orders/
      year=2025/
        month=01/
          part-0001.parquet
        month=02/
          part-0001.parquet
```

### Descoberta via manifest

Quando o Pipe grava dados, ele pode gerar um arquivo `.dataspoc/manifest.json` no bucket. O Lens verifica esse manifesto primeiro ao executar `add-bucket`.

O manifesto contem:

```json
{
  "tables": [
    {
      "table": "users",
      "location": "raw/users",
      "source": "pipe",
      "columns": ["id", "name", "email", "created_at"],
      "row_count": 1200
    },
    {
      "table": "orders",
      "location": "raw/orders",
      "source": "pipe",
      "columns": ["id", "user_id", "product_id", "quantity", "total", "status", "order_date", "created_at"],
      "row_count": 5430
    }
  ]
}
```

### Descoberta via scan (fallback)

Se nao houver manifesto, o Lens faz um scan recursivo do bucket buscando arquivos `*.parquet`. Ele agrupa os arquivos por diretorio para determinar as tabelas.

O nome da tabela e derivado do caminho relativo do diretorio:
- `raw/users/` vira a tabela `raw_users`
- `curated/orders_monthly/` vira a tabela `curated_orders_monthly`

### Hive partitioning

O Lens cria views com `hive_partitioning=true`, entao particoes no formato `year=2025/month=01/` sao automaticamente reconhecidas como colunas.

### Schema evolution

O parametro `union_by_name=true` e usado na criacao das views, permitindo que arquivos Parquet com schemas levemente diferentes (colunas adicionadas ao longo do tempo) sejam combinados corretamente.

---

## 12. Export

O Lens permite exportar resultados de queries em tres formatos. O export e uma flag (`--export` / `-e`) nos comandos `query` e `ask`. O formato e detectado automaticamente pela extensao do arquivo.

### Formatos suportados

| Formato  | Extensao   | Descricao                                    |
|----------|------------|----------------------------------------------|
| CSV      | `.csv`     | Texto delimitado por virgula, com header      |
| JSON     | `.json`    | Array de objetos JSON com indentacao          |
| Parquet  | `.parquet` | Formato colunar binario via DuckDB COPY TO    |

### Via CLI (flag --export)

```bash
# CSV
dataspoc-lens query "SELECT * FROM users" --export resultado.csv

# JSON
dataspoc-lens query "SELECT * FROM users" -e resultado.json

# Parquet
dataspoc-lens query "SELECT * FROM users" -e resultado.parquet

# Export a partir de pergunta em linguagem natural
dataspoc-lens ask "pedidos por cidade" --export cidades.csv
```

A mensagem de confirmacao exibe a quantidade de linhas exportadas:

```
Exported 1200 row(s) to resultado.csv (csv)
```

### Via shell (.export)

No shell interativo, primeiro execute uma query e depois exporte o resultado:

```
lens> SELECT * FROM orders WHERE status = 'pending';
(... resultado com 47 linhas ...)

lens> .export csv /tmp/pendentes.csv
Exported 47 rows to /tmp/pendentes.csv (csv)
```

O `.export` no shell sempre exporta o resultado da **ultima query executada**. Se voce nao executou nenhuma query, recebera a mensagem: "No previous query result to export."

### Detalhes de cada formato

**CSV:** gera um arquivo UTF-8 com header na primeira linha. Usa o modulo `csv` do Python.

**JSON:** gera um array de objetos, um por linha do resultado. Valores nao serializaveis sao convertidos para string. Usa indentacao de 2 espacos.

**Parquet:** para export via CLI, usa `COPY TO` nativo do DuckDB (mais eficiente). Para export via `.export` no shell, cria uma tabela temporaria e faz o COPY.

---

## 13. Cache Local

### O que e?

O cache local copia arquivos Parquet do bucket remoto (S3, GCS, Azure) para a maquina do analista. Isso evita consumo repetido de bandwidth e custos de egress do provedor de nuvem. Uma vez cacheada, a tabela e lida diretamente do disco local, sem acesso a rede.

### Comandos

#### Cachear uma tabela

```bash
dataspoc-lens cache <tabela>
```

Baixa todos os arquivos Parquet da tabela para o cache local. Se a tabela ja estiver cacheada e fresca, o comando nao faz nada (use `--refresh` para forcar).

**Exemplo:**

```bash
dataspoc-lens cache orders
# Caching 'orders'...
# Cached 'orders': 3 file(s), 42.5 MB
```

#### Listar tabelas cacheadas

```bash
dataspoc-lens cache --list
```

Exibe uma tabela com todas as tabelas cacheadas, incluindo data do cache, tamanho e status (fresh ou stale).

**Exemplo de saida:**

```
         Cached Tables
┏━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━┓
┃ Table    ┃ Cached At           ┃ Size     ┃ Status ┃
┡━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━┩
│ orders   │ 2026-03-20T14:30:00 │ 42.5 MB  │ fresh  │
│ users    │ 2026-03-18T09:15:00 │ 8.2 MB   │ stale  │
└──────────┴─────────────────────┴──────────┴────────┘
```

#### Forcar re-download

```bash
dataspoc-lens cache <tabela> --refresh
```

Remove o cache existente e baixa novamente os dados do bucket remoto. Util quando o status esta "stale" ou quando voce quer garantir que tem a versao mais recente.

#### Limpar cache

```bash
# Limpar cache de uma tabela especifica
dataspoc-lens cache <tabela> --clear

# Limpar todo o cache
dataspoc-lens cache --clear
```

Remove os arquivos Parquet locais e os metadados associados.

#### No shell interativo

Dentro do shell, use o dot command `.cache`:

```
lens> .cache orders
Caching 'orders'...
Cached 'orders': 3 file(s), 42.5 MB
```

### Como funciona

1. Os arquivos Parquet sao copiados do bucket remoto para `~/.dataspoc-lens/cache/<tabela>/`.
2. Um arquivo de metadados `cache_meta.json` e mantido em `~/.dataspoc-lens/cache/` com informacoes de cada tabela cacheada (data, tamanho, URI de origem, quantidade de arquivos).
3. Ao montar as views (`mount_views`), o Lens verifica se existe um cache local fresco para a tabela. Se existir, usa o path local automaticamente em vez de acessar o bucket remoto.
4. O download e feito via `fsspec`, suportando todos os provedores de nuvem (S3, GCS, Azure) e paths locais.

### Estrutura do cache

```
~/.dataspoc-lens/
  cache/
    cache_meta.json          # Metadados de todas as tabelas cacheadas
    orders/
      part-0001.parquet
      part-0002.parquet
      part-0003.parquet
    users/
      part-0001.parquet
```

### Freshness (validade do cache)

O Lens determina se um cache esta **fresh** (fresco) ou **stale** (desatualizado) comparando dois timestamps:

- **`cached_at`**: momento em que o cache foi criado (registrado em `cache_meta.json`).
- **`last_extraction`**: timestamp da ultima extracao registrada no manifesto do Pipe (`.dataspoc/manifest.json`).

Se o Pipe executou uma nova extracao **depois** do momento em que o cache foi criado, a tabela e marcada como **stale**. Isso significa que existem dados mais recentes no bucket que nao estao no cache local.

Se nao houver informacao de `last_extraction` no manifesto (por exemplo, dados nao gerados pelo Pipe), o cache e considerado **fresh**.

Para atualizar um cache stale, use `--refresh`:

```bash
dataspoc-lens cache orders --refresh
```

### Quando usar o cache

O cache local e recomendado nos seguintes cenarios:

- **Trabalho offline**: voce precisa analisar dados sem conexao com a internet ou com a rede do provedor.
- **Reduzir latencia**: queries sobre dados locais sao significativamente mais rapidas do que sobre dados remotos, especialmente para tabelas grandes.
- **Evitar custos de egress**: provedores de nuvem cobram por transferencia de dados para fora do bucket. Cachear localmente evita downloads repetidos.
- **Analise exploratoria intensiva**: se voce vai executar muitas queries sobre a mesma tabela, o cache evita re-leitura remota a cada execucao.
- **Ambientes com conexao instavel**: em redes lentas ou instáveis, o cache garante acesso confiavel aos dados.

**Importante:** o cache ocupa espaco em disco local. Use `dataspoc-lens cache --list` para monitorar o tamanho e `dataspoc-lens cache --clear` para liberar espaco quando necessario.

---

## 14. Troubleshooting

### Bucket inacessivel

**Sintoma:** Mensagem de erro ao fazer `add-bucket` ou `catalog`.

**Causas possiveis:**
- Credenciais de nuvem nao configuradas ou expiradas.
- URI do bucket incorreta (verifique s3://, gs://, az://).
- Bucket nao existe ou voce nao tem permissao de leitura.

**Solucao:**
- Verifique suas credenciais com o CLI do provedor (ex.: `aws s3 ls s3://meu-bucket/`).
- Certifique-se de que o extra correto esta instalado (ex.: `pip install dataspoc-lens[s3]`).
- Teste com um diretorio local primeiro: `dataspoc-lens add-bucket file:///tmp/test`.

### LLM nao configurado

**Sintoma:** Erro "Configure DATASPOC_LLM_API_KEY com sua chave de API."

**Solucao:** Isso so se aplica ao usar provedores cloud (anthropic ou openai):

```bash
export DATASPOC_LLM_API_KEY=sua-chave-aqui
```

Se preferir IA local sem chave de API, use o Ollama (padrao):

```bash
dataspoc-lens setup-ai
```

### Modulo de AI nao encontrado

**Sintoma:** Erro "Modulo de AI nao encontrado."

**Solucao:**

```bash
pip install dataspoc-lens[ai]
```

### JupyterLab nao instalado

**Sintoma:** Erro "JupyterLab nao encontrado."

**Solucao:**

```bash
pip install dataspoc-lens[jupyter]
```

### Nenhuma tabela encontrada

**Sintoma:** Mensagem "No tables found in this bucket."

**Causas possiveis:**
- O bucket nao contem arquivos `.parquet`.
- Os arquivos Parquet estao em subpastas muito profundas.
- O manifesto existe mas esta vazio ou malformado.

**Solucao:**
- Verifique que o bucket contem arquivos `.parquet`.
- Use `dataspoc-lens catalog --detail <nome>` para inspecionar tabelas individuais.

### httpfs nao disponivel

**Sintoma:** Erro ao consultar dados remotos, mencionando httpfs.

**Solucao:**
O Lens tenta instalar e carregar a extensao httpfs automaticamente. Se falhar:

```bash
# No Python
import duckdb
conn = duckdb.connect()
conn.execute("INSTALL httpfs;")
```

### SQL gerado pela IA esta incorreto

**Sintoma:** O comando `ask` gera SQL que falha ou retorna resultados inesperados.

**Solucao:**
- Use `--debug` para ver o prompt enviado e entender o contexto.
- Reformule a pergunta de forma mais especifica.
- Mencione nomes de tabelas e colunas explicitamente na pergunta.
- Execute o SQL manualmente e ajuste conforme necessario.

### Erro de permissao no diretorio de configuracao

**Sintoma:** Erro ao executar `init` ou `add-bucket`.

**Solucao:**
- Verifique permissoes em `~/.dataspoc-lens/`.
- Execute `chmod -R u+rw ~/.dataspoc-lens/` se necessario.

---

## 15. Exemplos Praticos

### Exemplo 1: Analise de vendas de e-commerce

Cenario: voce tem dados de pedidos e clientes gravados pelo Pipe em um bucket S3.

```bash
# 1. Inicializar
dataspoc-lens init

# 2. Registrar o bucket
dataspoc-lens add-bucket s3://ecommerce-data-lake

# 3. Verificar tabelas
dataspoc-lens catalog
# Resultado: users (5 colunas, 12000 linhas), orders (8 colunas, 54000 linhas), products (6 colunas, 320 linhas)

# 4. Ver schema detalhado
dataspoc-lens catalog --detail orders

# 5. Consultar via CLI
dataspoc-lens query "
  SELECT
    DATE_TRUNC('month', order_date) AS mes,
    COUNT(*) AS pedidos,
    SUM(total) AS receita,
    AVG(total) AS ticket_medio
  FROM orders
  WHERE status = 'completed'
  GROUP BY 1
  ORDER BY 1 DESC
  LIMIT 12
"

# 6. Exportar resultado
dataspoc-lens query "
  SELECT u.name, u.email, COUNT(o.id) AS pedidos, SUM(o.total) AS total_gasto
  FROM users u
  JOIN orders o ON o.user_id = u.id
  WHERE o.status = 'completed'
  GROUP BY u.name, u.email
  ORDER BY total_gasto DESC
  LIMIT 100
" --export top_clientes.csv

# 7. Perguntar em linguagem natural
dataspoc-lens ask "quais os 5 produtos com maior receita no ultimo trimestre?"
```

### Exemplo 2: Pipeline de dados com transforms

Cenario: voce quer limpar dados brutos e criar tabelas derivadas para um dashboard.

```bash
# 1. Setup
dataspoc-lens init
dataspoc-lens add-bucket file:///dados/raw
```

Crie os transforms em `~/.dataspoc-lens/transforms/`:

**`001_clean_customers.sql`**
```sql
CREATE OR REPLACE TABLE clean_customers AS
SELECT
    id,
    TRIM(name) AS name,
    LOWER(TRIM(email)) AS email,
    phone,
    created_at
FROM raw_customers
WHERE email IS NOT NULL
  AND email LIKE '%@%.%';
```

**`002_daily_revenue.sql`**
```sql
CREATE OR REPLACE TABLE daily_revenue AS
SELECT
    CAST(order_date AS DATE) AS date,
    COUNT(*) AS orders,
    SUM(total) AS revenue,
    COUNT(DISTINCT user_id) AS unique_buyers,
    AVG(total) AS avg_ticket
FROM raw_orders
WHERE status IN ('completed', 'shipped')
GROUP BY 1
ORDER BY 1;
```

**`003_customer_segments.sql`**
```sql
CREATE OR REPLACE TABLE customer_segments AS
SELECT
    c.id,
    c.name,
    c.email,
    COUNT(o.id) AS lifetime_orders,
    SUM(o.total) AS lifetime_value,
    MAX(o.order_date) AS last_purchase,
    CASE
        WHEN SUM(o.total) > 5000 THEN 'VIP'
        WHEN SUM(o.total) > 1000 THEN 'Regular'
        ELSE 'Occasional'
    END AS segment
FROM clean_customers c
LEFT JOIN raw_orders o ON o.user_id = c.id
GROUP BY c.id, c.name, c.email;
```

```bash
# 2. Verificar transforms
dataspoc-lens transform list

# 3. Executar
dataspoc-lens transform run

# 4. Consultar resultados
dataspoc-lens shell
```

```
lens> .tables
  raw_customers (VIEW)
  raw_orders (VIEW)
  clean_customers (TABLE)
  daily_revenue (TABLE)
  customer_segments (TABLE)

lens> SELECT segment, COUNT(*) AS total, AVG(lifetime_value) AS avg_ltv
      FROM customer_segments
      GROUP BY segment
      ORDER BY avg_ltv DESC;

+------------+-------+----------+
| segment    | total | avg_ltv  |
+------------+-------+----------+
| VIP        | 45    | 8234.50  |
| Regular    | 312   | 2156.30  |
| Occasional | 843   | 287.40   |
+------------+-------+----------+
(3 row(s), 0.012s)

lens> .export csv /tmp/segments.csv
Exported 3 rows to /tmp/segments.csv (csv)
```

### Exemplo 3: Exploracao interativa com IA e Jupyter

Cenario: voce quer explorar um dataset novo rapidamente, combinando shell, IA e Jupyter.

```bash
# 1. Setup
dataspoc-lens init
dataspoc-lens add-bucket gs://analytics-lake/events

# 2. Descobrir o que tem no bucket
dataspoc-lens catalog

# 3. Explorar com IA (sem saber os nomes das colunas)
dataspoc-lens setup-ai
dataspoc-lens ask "mostre um resumo dos dados disponiveis"
dataspoc-lens ask "quantos eventos unicos existem por tipo?"
dataspoc-lens ask "qual a distribuicao de eventos por dia da semana?"

# 4. Usar --debug para entender melhor
dataspoc-lens ask --debug "quais eventos tem taxa de conversao acima de 5%?"

# 5. Abrir Jupyter para analise mais profunda
pip install dataspoc-lens[jupyter]
dataspoc-lens notebook
```

No Jupyter:

```python
# Celula 1 - As tabelas ja estao montadas
%%sql
SELECT event_type, COUNT(*) AS total
FROM events
GROUP BY event_type
ORDER BY total DESC
LIMIT 20
```

```python
# Celula 2 - Usar pandas para visualizacao
df = conn.execute("""
    SELECT
        CAST(event_time AS DATE) AS date,
        event_type,
        COUNT(*) AS count
    FROM events
    WHERE event_time >= CURRENT_DATE - INTERVAL 30 DAY
    GROUP BY 1, 2
""").fetchdf()

import matplotlib.pyplot as plt
df.pivot(index='date', columns='event_type', values='count').plot(figsize=(14, 6))
plt.title('Eventos por Dia (Ultimos 30 dias)')
plt.show()
```

```python
# Celula 3 - Exportar para analise posterior
%%sql
COPY (
    SELECT * FROM events
    WHERE event_type = 'purchase'
    AND event_time >= '2025-01-01'
) TO '/tmp/purchases_2025.parquet' (FORMAT PARQUET)
```

---

*DataSpoc Lens e parte da plataforma DataSpoc. Licenca: Apache-2.0.*
