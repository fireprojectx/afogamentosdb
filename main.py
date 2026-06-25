from fastapi import FastAPI
from fastapi.responses import Response, JSONResponse
import pandas as pd
import os
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

import extrair as ex

app = FastAPI()

DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)


@app.get("/dados_afogamentos")
def get_dados():
    df = ex.extrair()
    if df.empty:
        return JSONResponse(status_code=502, content={"erro": "TabNet não retornou dados"})
    df.to_sql("obitos", engine, if_exists="replace", index=False)
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
        headers={"Content-Disposition": "attachment; filename=obitos.csv"},
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
