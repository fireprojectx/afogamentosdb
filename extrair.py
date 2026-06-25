"""Extrai óbitos por afogamento do TabNet-MG/SIM e retorna um DataFrame consolidado."""
import requests
import pandas as pd
from io import StringIO

_URLS = {
    ("Menor de 1 ano", "Masculino"): "http://tabnet.saude.mg.gov.br/csv/A15561110_14_8_2.csv",
    ("1 a 4 anos", "Masculino"): "http://tabnet.saude.mg.gov.br/csv/A15570810_14_8_2.csv",
    ("5 a 9 anos", "Masculino"): "http://tabnet.saude.mg.gov.br/csv/A15573810_14_8_2.csv",
    ("10 a 14 anos", "Masculino"): "http://tabnet.saude.mg.gov.br/csv/A15581210_14_8_2.csv",
    ("15 a 19 anos", "Masculino"): "http://tabnet.saude.mg.gov.br/csv/A15584110_14_8_2.csv",
    ("20 a 29 anos", "Masculino"): "http://tabnet.saude.mg.gov.br/csv/A15590710_14_8_2.csv",
    ("30 a 39 anos", "Masculino"): "http://tabnet.saude.mg.gov.br/csv/A16011510_14_8_2.csv",
    ("40 a 49 anos", "Masculino"): "http://tabnet.saude.mg.gov.br/csv/A16014010_14_8_2.csv",
    ("50 a 59 anos", "Masculino"): "http://tabnet.saude.mg.gov.br/csv/A16020510_14_8_2.csv",
    ("60 a 69 anos", "Masculino"): "http://tabnet.saude.mg.gov.br/csv/A16022510_14_8_2.csv",
    ("70 a 79 anos", "Masculino"): "http://tabnet.saude.mg.gov.br/csv/A16025610_14_8_2.csv",
    ("80 e mais", "Masculino"): "http://tabnet.saude.mg.gov.br/csv/A16031510_14_8_2.csv",
    ("Idade ignorada", "Masculino"): "http://tabnet.saude.mg.gov.br/csv/A16033410_14_8_2.csv",
    ("Menor de 1 ano", "Feminino"): "http://tabnet.saude.mg.gov.br/csv/A15463510_14_8_2.csv",
    ("1 a 4 anos", "Feminino"): "http://tabnet.saude.mg.gov.br/csv/A15470710_14_8_2.csv",
    ("5 a 9 anos", "Feminino"): "http://tabnet.saude.mg.gov.br/csv/A15472510_14_8_2.csv",
    ("10 a 14 anos", "Feminino"): "http://tabnet.saude.mg.gov.br/csv/A15474810_14_8_2.csv",
    ("15 a 19 anos", "Feminino"): "http://tabnet.saude.mg.gov.br/csv/A15481110_14_8_2.csv",
    ("20 a 29 anos", "Feminino"): "http://tabnet.saude.mg.gov.br/csv/A15482910_14_8_2.csv",
    ("30 a 39 anos", "Feminino"): "http://tabnet.saude.mg.gov.br/csv/A15484910_14_8_2.csv",
    ("40 a 49 anos", "Feminino"): "http://tabnet.saude.mg.gov.br/csv/A15494010_14_8_2.csv",
    ("50 a 59 anos", "Feminino"): "http://tabnet.saude.mg.gov.br/csv/A15501310_14_8_2.csv",
    ("60 a 69 anos", "Feminino"): "http://tabnet.saude.mg.gov.br/csv/A15504910_14_8_2.csv",
    ("70 a 79 anos", "Feminino"): "http://tabnet.saude.mg.gov.br/csv/A15510810_14_8_2.csv",
    ("80 e mais", "Feminino"): "http://tabnet.saude.mg.gov.br/csv/A15513110_14_8_2.csv",
    ("Idade ignorada", "Feminino"): "http://tabnet.saude.mg.gov.br/csv/A15515110_14_8_2.csv",
}


def extrair() -> pd.DataFrame:
    """Baixa todos os CSVs do TabNet-MG e retorna DataFrame consolidado."""
    todos = []
    for (faixa, sexo), url in _URLS.items():
        try:
            r = requests.get(url, timeout=30)
            r.encoding = "latin1"
            df = pd.read_csv(StringIO(r.text), sep=";", skiprows=6, skipfooter=8, engine="python")
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
            todos.append(
                df_long[["Codigo municipio", "municipio", "mês", "ano", "sexo", "faixa etária", "Óbitos"]]
            )
        except Exception as e:
            print(f"Erro ao processar {faixa} / {sexo}: {e}")
    if not todos:
        return pd.DataFrame()
    return pd.concat(todos, ignore_index=True)
