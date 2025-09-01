from fastapi import FastAPI
from fastapi.responses import Response
import pandas as pd
import requests
from io import StringIO
import os
from sqlalchemy import create_engine
from sqlalchemy import text
from fastapi.responses import JSONResponse
from sqlalchemy.exc import SQLAlchemyError

app = FastAPI()

DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)

@app.get("/dados_afogamentos")
def get_dados():
    urls = {
        ("Menor de 1 ano", "Masculino"): "http://tabnet.saude.mg.gov.br/csv/A13593810_14_8_2.csv",
        ("1 a 4 anos", "Masculino"): "http://tabnet.saude.mg.gov.br/csv/A16240910_14_8_2.csv",
        ("5 a 9 anos", "Masculino"): "http://tabnet.saude.mg.gov.br/csv/A16243510_14_8_2.csv",
        ("10 a 14 anos", "Masculino"): "http://tabnet.saude.mg.gov.br/csv/A16375810_14_8_2.csv",
        ("15 a 19 anos", "Masculino"): "http://tabnet.saude.mg.gov.br/csv/A10394910_14_8_2.csv",
        ("20 a 29 anos", "Masculino"): "http://tabnet.saude.mg.gov.br/csv/A16391710_14_8_2.csv",
        ("30 a 39 anos", "Masculino"): "http://tabnet.saude.mg.gov.br/csv/A16394710_14_8_2.csv",
        ("40 a 49 anos", "Masculino"): "http://tabnet.saude.mg.gov.br/csv/A16401610_14_8_2.csv",
        ("50 a 59 anos", "Masculino"): "http://tabnet.saude.mg.gov.br/csv/A16404610_14_8_2.csv",
        ("60 a 69 anos", "Masculino"): "http://tabnet.saude.mg.gov.br/csv/A10424610_14_8_2.csv",
        ("70 a 79 anos", "Masculino"): "http://tabnet.saude.mg.gov.br/csv/A14050710_14_8_2.csv",
        ("80 e mais", "Masculino"): "http://tabnet.saude.mg.gov.br/csv/A16415110_14_8_2.csv",
        ("Idade ignorada", "Masculino"): "http://tabnet.saude.mg.gov.br/csv/A16421310_14_8_2.csv",
        ("Menor de 1 ano", "Feminino"): "http://tabnet.saude.mg.gov.br/csv/A16464010_14_8_2.csv",
        ("1 a 4 anos", "Feminino"): "http://tabnet.saude.mg.gov.br/csv/A16472110_14_8_2.csv",
        ("5 a 9 anos", "Feminino"): "http://tabnet.saude.mg.gov.br/csv/A16474610_14_8_2.csv",
        ("10 a 14 anos", "Feminino"): "http://tabnet.saude.mg.gov.br/csv/A16481810_14_8_2.csv",
        ("15 a 19 anos", "Feminino"): "http://tabnet.saude.mg.gov.br/csv/A16483910_14_8_2.csv",
        ("20 a 29 anos", "Feminino"): "http://tabnet.saude.mg.gov.br/csv/A16490110_14_8_2.csv",
        ("30 a 39 anos", "Feminino"): "http://tabnet.saude.mg.gov.br/csv/A16492010_14_8_2.csv",
        ("40 a 49 anos", "Feminino"): "http://tabnet.saude.mg.gov.br/csv/A16493910_14_8_2.csv",
        ("50 a 59 anos", "Feminino"): "http://tabnet.saude.mg.gov.br/csv/A13412610_14_8_2.csv",
        ("60 a 69 anos", "Feminino"): "http://tabnet.saude.mg.gov.br/csv/A16501910_14_8_2.csv",
        ("70 a 79 anos", "Feminino"): "http://tabnet.saude.mg.gov.br/csv/A16503610_14_8_2.csv",
        ("80 e mais", "Feminino"): "http://tabnet.saude.mg.gov.br/csv/A10502810_14_8_2.csv",
        ("Idade ignorada", "Feminino"): "http://tabnet.saude.mg.gov.br/csv/A16512010_14_8_2.csv",
    }

    todos_dfs = []

    for (faixa, sexo), url in urls.items():
        try:
            r = requests.get(url)
            r.encoding = 'latin1'
            df = pd.read_csv(StringIO(r.text), sep=";", skiprows=6, skipfooter=8, engine="python")
            if "Total" in df.columns:
                df.drop(columns=["Total"], inplace=True)
            df.replace("-", 0, inplace=True)
            df[['Codigo municipio', 'municipio']] = df['Município'].str.extract(r'"?(\d+)"?\s*(.*)')
            df.drop(columns=["Município"], inplace=True)
            df_long = df.melt(id_vars=["Codigo municipio", "municipio"], var_name="mes_ano", value_name="Óbitos")
            df_long["mes_ano"] = df_long["mes_ano"].str.replace(r"[.]{2}", "", regex=True)
            df_long[["mês", "ano"]] = df_long["mes_ano"].str.extract(r'(\w+)\/(\d{4})')
            df_long = df_long.dropna(subset=["Codigo municipio", "municipio"])
            df_long["Óbitos"] = pd.to_numeric(df_long["Óbitos"], errors="coerce").fillna(0).astype(int)
            df_long["sexo"] = sexo
            df_long["faixa etária"] = faixa
            df_final = df_long[["Codigo municipio", "municipio", "mês", "ano", "sexo", "faixa etária", "Óbitos"]]
            todos_dfs.append(df_final)
        except Exception as e:
            print(f"Erro ao processar {faixa}, {sexo}: {e}")
            continue

    df_total = pd.concat(todos_dfs, ignore_index=True)
    df_total.to_sql("obitos", engine, if_exists="replace", index=False)

    return {"status": "Dados salvos no banco de dados PostgreSQL com sucesso."}

@app.get("/consultar_dados")
def consultar_dados(limite: int = 100, offset: int = 0):
    query = f"SELECT * FROM obitos LIMIT {limite} OFFSET {offset}"
    df = pd.read_sql(query, engine)
    return {"dados": df.to_dict(orient="records")}


@app.get("/exportar_csv")
def exportar_csv():
    df = pd.read_sql("SELECT * FROM obitos", engine)
    return Response(
        content=df.to_csv(index=False),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=obitos.csv"}
    )

@app.get("/total_obitos")
def total_obitos():
    try:
        with engine.connect() as conn:
            result = conn.execute(text('SELECT SUM("Óbitos") FROM obitos'))
            total = result.scalar()
        return {"total_obitos": total}
    except SQLAlchemyError as e:
        return JSONResponse(status_code=500, content={"erro": str(e)})
