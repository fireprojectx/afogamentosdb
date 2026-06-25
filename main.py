"""API opcional do afogamentosdb (FastAPI).

Serve o obitos.csv (gerado por extrair.py / pelo GitHub Action). Não exige banco.
- GET /              status
- GET /atualizar     refaz a extração ao vivo do TabNet e regrava obitos.csv
- GET /dados         JSON paginado
- GET /exportar_csv  download do CSV
"""
import os

import pandas as pd
from fastapi import FastAPI
from fastapi.responses import JSONResponse, Response

import extrair as ex

app = FastAPI(title="afogamentosdb", description="Óbitos por afogamento MG (SIM/TabNet)")
CSV = "obitos.csv"


@app.get("/")
def root():
    existe = os.path.exists(CSV)
    return {"ok": True, "fonte": "TabNet-MG / SIM (POST dinâmico)",
            "csv_disponivel": existe,
            "raw": "https://raw.githubusercontent.com/fireprojectx/afogamentosdb/main/obitos.csv"}


@app.get("/atualizar")
def atualizar():
    df = ex.extrair()
    if df.empty:
        return JSONResponse(status_code=502, content={"erro": "TabNet não retornou dados"})
    df.to_csv(CSV, index=False, encoding="utf-8")
    return {"status": "atualizado", "linhas": len(df), "obitos": int(df["Óbitos"].sum())}


@app.get("/dados")
def dados(limite: int = 100, offset: int = 0):
    if not os.path.exists(CSV):
        return JSONResponse(status_code=404, content={"erro": "rode /atualizar primeiro"})
    df = pd.read_csv(CSV)
    return {"total": len(df), "dados": df.iloc[offset:offset + limite].to_dict(orient="records")}


@app.get("/exportar_csv")
def exportar_csv():
    if not os.path.exists(CSV):
        return JSONResponse(status_code=404, content={"erro": "rode /atualizar primeiro"})
    with open(CSV, encoding="utf-8") as f:
        return Response(content=f.read(), media_type="text/csv",
                        headers={"Content-Disposition": "attachment; filename=obitos.csv"})
