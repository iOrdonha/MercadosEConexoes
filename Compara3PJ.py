#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script: Compara3PJ.py
Descrição: Gera DDA (Dicionário de Dados ANEEL) para UC (AT, MT e BT) com nome do campo, tipo inferido pelo Pandas e tipo_aneel sugerido.
          Ajusta tipos: NULL/zeros e corrige POINT_X/POINT_Y como REAL.
          Verifica existência de diretórios e arquivos antes de processar.
Data: 2025-05-22
Programador: Ivo Cyrillo
"""

import sys
import pandas as pd
import zipfile
import gc
import traceback
from pathlib import Path

# === Configurações ===
INPUT_DIR  = Path(r"C:/Users/ivocy/Downloads")
OUTPUT_DIR = Path(r"C:/Projetos/TotalEnergie/BaseDados")
ENCODING   = "latin1"
SEP        = ";"
DECIMAL    = ","
TEST_MODE  = False  # True para rodar testes interativos de leitura/processamento

FILES = {
    "at": INPUT_DIR / "ucat_pj.csv",
    "mt": INPUT_DIR / "ucmt_pj.csv",
    "bt": INPUT_DIR / "ucbt_pj.zip",
}


def map_tipo_aneel(campo: str) -> str:
    """Define tipo_aneel com base em prefixos e nome do campo."""
    c = campo.lower()
    # INTEGER
    if c.startswith("fic_"):
        return "INTEGER"
    # REAL
    if c.startswith(("dic_", "dem_", "ene_")) or c in ("point_x", "point_y"):
        return "REAL"
    # TEXT
    return "TEXT"


def verify_environment():
    """Verifica se diretórios e arquivos necessários existem antes de processar."""
    missing = []
    if not INPUT_DIR.exists() or not INPUT_DIR.is_dir():
        missing.append(f"Diretório de input não encontrado: {INPUT_DIR}")
    for tipo, path in FILES.items():
        if not path.exists():
            missing.append(f"Arquivo não encontrado ({tipo}): {path}")
    if missing:
        for msg in missing:
            print(f"[ERRO] {msg}")
        sys.exit(1)
    print("Diretórios e arquivos encontrados\n")


def processar_tipo(tipo: str, path: Path):
    print(f"Processando '{tipo}' → {path.name}")
    try:
        # 1) Leitura de até 10k linhas
        if tipo in ("at", "mt"):
            df = pd.read_csv(
                path,
                sep=SEP,
                decimal=DECIMAL,
                encoding=ENCODING,
                nrows=10000,
                low_memory=False
            )
        else:
            with zipfile.ZipFile(path) as z:
                csv_name = next(f for f in z.namelist() if f.lower().endswith(".csv"))
                with z.open(csv_name) as f:
                    df = pd.read_csv(
                        f,
                        sep=SEP,
                        decimal=DECIMAL,
                        encoding=ENCODING,
                        nrows=10000,
                        low_memory=False
                    )

        # 2) Ajuste de nulos e zeros conforme regra:
        tipo_map = {col: map_tipo_aneel(col) for col in df.columns}
        for col, t in tipo_map.items():
            if t in ("REAL", "INTEGER"):
                # converter para numérico e preencher NaN com 0
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
            else:
                # texto: manter NaN para nulos
                df[col] = df[col].where(df[col].notna(), pd.NA)

        # 3) Teste opcional
        if TEST_MODE:
            print(f"  [TEST] Linhas lidas: {len(df)}")
            print(f"  [TEST] Colunas: {list(df.columns)}")
            print(f"  [TEST] Tipos pandas após ajuste:")
            print(df.dtypes.to_string())

        # 4) Montar DDA
        rows = []
        for col in df.columns:
            pandas_dtype = df[col].dtype.name
            tipo_aneel = tipo_map[col]
            rows.append({
                "campo": col,
                "pandas_dtype": pandas_dtype,
                "tipo_aneel": tipo_aneel
            })
        dda_df = pd.DataFrame(rows)

        # 5) Exportar DDA
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        out_path = OUTPUT_DIR / f"DDA_ANEEL_uc{tipo}_pj.csv"
        dda_df.to_csv(out_path, sep=";", encoding=ENCODING, index=False)
        print(f"Exportado: {out_path}\n")

    except Exception as e:
        print(f"[ERROR] ao processar {tipo}: {e}")
        traceback.print_exc()

    finally:
        # 6) Limpar memória
        if 'df' in locals():
            del df
        gc.collect()


if __name__ == "__main__":
    verify_environment()
    for tp, p in FILES.items():
        processar_tipo(tp, p)
    print("DDA gerados para AT, MT e BT.")