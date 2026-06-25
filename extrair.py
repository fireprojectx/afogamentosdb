"""Extrai óbitos por afogamento (CID W65-W74) do TabNet-MG/SIM via POST dinâmico."""
import re

import pandas as pd
import requests
from io import StringIO

_CGI = "http://tabnet.saude.mg.gov.br/cgi/tabcgi.exe?sim/cnv/obt10mg.def"
_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; afogamentosdb)",
    "Referer": _CGI,
}

_SEXOS = ["Masculino", "Feminino"]

_FAIXAS = [
    "Menor de 1 ano", "1 a 4 anos", "5 a 9 anos", "10 a 14 anos",
    "15 a 19 anos", "20 a 29 anos", "30 a 39 anos", "40 a 49 anos",
    "50 a 59 anos", "60 a 69 anos", "70 a 79 anos", "80 e mais", "Idade ignorada",
]

# CID-10 W65-W74: afogamento e submersão acidentais
_CAUSAS = " ".join(f"W{i}" for i in range(65, 75))


def _post_tabnet(session: requests.Session, sexo: str, faixa: str) -> str:
    """Envia POST ao TabNet e devolve o conteúdo CSV bruto."""
    payload = {
        "Linha": "Município",
        "Coluna": "Mês_do_Óbito",
        "Conteúdo": "Óbitos_p/_Residentes",
        "SExo": sexo,
        "SFaixEtar": faixa,
        "SCausa": _CAUSAS,
        "mostre": "Mostra",
    }
    r = session.post(_CGI, data=payload, headers=_HEADERS, timeout=60)
    r.encoding = "latin1"

    # TabNet embute CSV dentro de <pre> no HTML da resposta
    match = re.search(r"<pre>(.*?)</pre>", r.text, re.DOTALL | re.IGNORECASE)
    if match:
        return match.group(1).strip()

    # Fallback: resposta já é CSV puro
    return r.text


def _parse(raw: str, sexo: str, faixa: str) -> pd.DataFrame:
    df = pd.read_csv(StringIO(raw), sep=";", skiprows=6, skipfooter=8, engine="python")
    if "Total" in df.columns:
        df.drop(columns=["Total"], inplace=True)
    df.replace("-", 0, inplace=True)
    df[["Codigo municipio", "municipio"]] = df["Município"].str.extract(r'"?(\d+)"?\s*(.*)')
    df.drop(columns=["Município"], inplace=True)
    df_long = df.melt(
        id_vars=["Codigo municipio", "municipio"],
        var_name="mes_ano",
        value_name="Óbitos",
    )
    df_long["mes_ano"] = df_long["mes_ano"].str.replace(r"[.]{2}", "", regex=True)
    df_long[["mês", "ano"]] = df_long["mes_ano"].str.extract(r"(\w+)\/(\d{4})")
    df_long = df_long.dropna(subset=["Codigo municipio", "municipio"])
    df_long["Óbitos"] = pd.to_numeric(df_long["Óbitos"], errors="coerce").fillna(0).astype(int)
    df_long["sexo"] = sexo
    df_long["faixa etária"] = faixa
    return df_long[["Codigo municipio", "municipio", "mês", "ano", "sexo", "faixa etária", "Óbitos"]]


def extrair() -> pd.DataFrame:
    """Consulta o TabNet por faixa etária × sexo e retorna DataFrame consolidado."""
    todos = []
    with requests.Session() as s:
        for sexo in _SEXOS:
            for faixa in _FAIXAS:
                try:
                    raw = _post_tabnet(s, sexo, faixa)
                    todos.append(_parse(raw, sexo, faixa))
                except Exception as e:
                    print(f"Erro ao processar {faixa} / {sexo}: {e}")
    if not todos:
        return pd.DataFrame()
    return pd.concat(todos, ignore_index=True)


if __name__ == "__main__":
    _RAILWAY = "https://web-production-0c693.up.railway.app"
    _CSV = "obitos.csv"

    print("Extraindo dados do TabNet...")
    df = extrair()
    if df.empty:
        print("Nenhum dado retornado — abortando.")
        raise SystemExit(1)

    df.to_csv(_CSV, index=False, encoding="utf-8")
    print(f"Salvo {_CSV} com {len(df)} linhas.")

    print("Enviando CSV ao Railway para atualizar o banco...")
    with open(_CSV, "rb") as f:
        resp = requests.post(
            f"{_RAILWAY}/importar_csv",
            files={"file": ("obitos.csv", f, "text/csv")},
            timeout=120,
        )
    print(f"Status Railway: {resp.status_code} — {resp.text}")
