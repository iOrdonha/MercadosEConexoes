#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script: CriaePopulaSQLitePJ.py
Descrição: Importa dados de UC (AT, MT, BT) na base SQLite usando DDA ANEEL para definição de tipos,
           aplicando conversões de zeros/NaN e formatação de data.
Data: 2025-05-22
Programador: Ivo Cyrillo
"""

import sys
import zipfile
import gc
from pathlib import Path
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine, types, text

# === Configurações ===
INPUT_DIR     = Path(r"C:/Users/ivocy/Downloads")
DDA_DIR       = Path(r"C:/Projetos/TotalEnergie/BaseDados")  # onde DDA foi salvo
OUTPUT_DIR    = Path(r"C:/Projetos/TotalEnergie/BaseDados")
DB_NAME       = "mercadoucpj.db"
DB_PATH       = OUTPUT_DIR / DB_NAME

RECREATE_DB   = True  # True apaga base existente e recria
ENCODING      = "latin1"
SEP           = ";"
DECIMAL       = ","
CHUNKSIZE     = 10000
DATE_FIELDS   = ["DATA_BASE"]  # campos de data a converter

# Map tipo_aneel -> SQLAlchemy
SQL_TYPE_MAP = {
    "INTEGER": types.INTEGER,
    "REAL":    types.REAL,
    "TEXT":    types.TEXT,
}

# Arquivos de dados por tipo
FILES = {
    "at": INPUT_DIR / "ucat_pj.csv",
    "mt": INPUT_DIR / "ucmt_pj.csv",
    "bt": INPUT_DIR / "ucbt_pj.zip",
}
# Arquivos DDA por tipo
DDA_FILES = {
    tp: DDA_DIR / f"DDA_ANEEL_uc{tp}_pj.csv"
    for tp in FILES
}


def verify_files():
    missing = []
    # Diretórios
    if not INPUT_DIR.is_dir():
        missing.append(f"Input dir não existe: {INPUT_DIR}")
    if not DDA_DIR.is_dir():
        missing.append(f"DDA dir não existe: {DDA_DIR}")
    # Dados e DDA
    for tp, path in FILES.items():
        if not path.exists(): missing.append(f"Arquivo de dados faltando ({tp}): {path}")
    for tp, path in DDA_FILES.items():
        if not path.exists(): missing.append(f"DDA faltando ({tp}): {path}")
    if missing:
        for m in missing: print(f"[ERRO] {m}")
        sys.exit(1)
    print("Diretórios e arquivos necessários encontrados.\n")


def load_dda(tp: str):
    """Carrega DDA para tipo e retorna dict campo->tipo_aneel"""
    dda_df = pd.read_csv(DDA_FILES[tp], sep=SEP, encoding=ENCODING, low_memory=False)
    return dict(zip(dda_df['campo'], dda_df['tipo_aneel']))


def read_data_chunk(tp: str, path, chunksize):
    """Retorna iterator de DataFrame por chunks."""
    if tp in ('at','mt'):
        return pd.read_csv(path, sep=SEP, decimal=DECIMAL,
                           encoding=ENCODING, chunksize=chunksize, iterator=True, low_memory=False)
    # BT como ZIP
    z = zipfile.ZipFile(path)
    csv_name = next(f for f in z.namelist() if f.lower().endswith('.csv'))
    return pd.read_csv(z.open(csv_name), sep=SEP, decimal=DECIMAL,
                       encoding=ENCODING, chunksize=chunksize, iterator=True, low_memory=False)


def parse_date(val: str) -> str:
    """Converte '31DEC2023:00:00:00.0000000' → '2023-12-31T00:00:00'"""
    try:
        dt = datetime.strptime(val, '%d%b%Y:%H:%M:%S.%f')
        return dt.strftime('%Y-%m-%dT%H:%M:%S')
    except Exception:
        return None


def importa_tipo(tp: str):
    print(f"Importando tipo {tp}...")
    # carregar DDA
    tipo_map = load_dda(tp)
    # preparar dtype para to_sql
    dtype_map = {col: SQL_TYPE_MAP[tipo_map[col]]()
                 for col in tipo_map}

    # read chunks
    reader = read_data_chunk(tp, FILES[tp], CHUNKSIZE)
    first = True
    batch = 0
    for chunk in reader:
        batch += 1
        # aplicar conversão de nulos/zeros e datas
        for col, t in tipo_map.items():
            if t in ('REAL','INTEGER'):
                chunk[col] = pd.to_numeric(chunk[col], errors='coerce').fillna(0)
            elif col in DATE_FIELDS:
                chunk[col] = chunk[col].apply(parse_date)
            else:
                chunk[col] = chunk[col].where(chunk[col].notna(), None)
        # inserir
        if first:
            chunk.to_sql(f'uc_{tp}_pj', engine, if_exists='replace', index=False, dtype=dtype_map)
            first = False
        else:
            chunk.to_sql(f'uc_{tp}_pj', engine, if_exists='append', index=False)
        # limpar
        del chunk; gc.collect()
    print(f"-> {tp} concluído, {batch} batches importados.")


if __name__ == '__main__':
    verify_files()
    # recriar DB se necessário
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    if RECREATE_DB and DB_PATH.exists():
        DB_PATH.unlink()
    # criar engine
    engine = create_engine(f'sqlite:///{DB_PATH}')
    # importar AT, MT, BT
    for tp in ('at','mt','bt'):
        importa_tipo(tp)
    print("Importação completa. DB em:", DB_PATH)