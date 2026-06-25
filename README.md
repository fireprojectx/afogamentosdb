# afogamentosdb

API de óbitos por afogamento em Minas Gerais, com dados extraídos do TabNet-MG/SIM (CID-10 W65–W74).

**Base URL:** `https://web-production-0c693.up.railway.app`

---

## Endpoints

| Método | Rota | Descrição |
|--------|------|-----------|
| GET | `/dados_afogamentos` | Extrai dados ao vivo do TabNet e salva no banco |
| GET | `/consultar_dados` | Retorna registros paginados do banco |
| GET | `/exportar_csv` | Download do CSV com todos os dados do banco |
| GET | `/total_obitos` | Total geral de óbitos |
| POST | `/importar_csv` | Recebe um CSV e salva no banco (usado pelo workflow) |

### Parâmetros de `/consultar_dados`

| Parâmetro | Padrão | Descrição |
|-----------|--------|-----------|
| `limite` | 100 | Número de registros por página |
| `offset` | 0 | Posição inicial |

---

## Atualização automática (GitHub Actions)

O banco é atualizado **toda segunda-feira às 06:00 UTC (03:00 horário de Brasília)** pelo workflow `.github/workflows/atualizar.yml`.

### O que acontece a cada execução

1. **Extração** — o script `extrair.py` faz 26 requisições POST ao TabNet-MG, uma por combinação de faixa etária × sexo, filtrando os CIDs W65–W74 (afogamento e submersão acidentais).
2. **Geração do CSV** — os dados consolidados são salvos em `obitos.csv`.
3. **Atualização do banco** — o CSV é enviado ao endpoint `POST /importar_csv` no Railway, que substitui a tabela `obitos` no PostgreSQL.
4. **Commit** — o `obitos.csv` é commitado no repositório como snapshot histórico.

### Acionar manualmente

1. Acesse a aba **Actions** no repositório.
2. Selecione o workflow **"Atualizar dados TabNet"**.
3. Clique em **"Run workflow"** → **"Run workflow"**.

---

## Atualização manual via API

Para forçar uma extração diretamente pelo Railway (sem passar pelo GitHub Actions):

```bash
curl -X GET https://web-production-0c693.up.railway.app/dados_afogamentos
```

Aguarde o retorno — a extração pode levar alguns minutos.

---

## Estrutura do projeto

```
afogamentosdb/
├── main.py           # API FastAPI (endpoints + conexão PostgreSQL)
├── extrair.py        # Extração do TabNet via POST dinâmico
├── requirements.txt  # Dependências Python
├── Procfile          # Comando de inicialização (Railway/Heroku)
└── .github/
    └── workflows/
        └── atualizar.yml  # Agendamento semanal (toda segunda)
```

## Dependências

```
fastapi
uvicorn[standard]
pandas
requests
sqlalchemy
psycopg2-binary
```
