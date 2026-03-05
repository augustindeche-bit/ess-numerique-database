"""
Identification des structures ESS avec un code NAF numérique

Croise la liste ESS France (entreprisesess.csv) avec la base Sirene
(StockUniteLegale_utf8.parquet) en filtrant sur 10 codes NAF numériques.

Sources  : ess_france (SIREN) + sirene (NAF)
Jointure : siren_direct — intersection SIREN ESS ∩ codes NAF numériques

Entrées :
    - data/raw/entreprisesess.csv             (SIREN ESS)
    - data/raw/StockUniteLegale_utf8.parquet  (Stock INSEE ~30M lignes)

Sortie :
    - data/processed/candidates/01_ess_x_naf.csv

Usage : python src/01_Candidates/01_ess_x_naf.py
"""

import sys
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from src.models import NAF_NUMERIQUE, Source, MethodeJointure

ROOT = Path(__file__).resolve().parents[2]
ESS_PATH = ROOT / "data" / "raw" / "entreprisesess.csv"
SIRENE_PATH = ROOT / "data" / "raw" / "StockUniteLegale_utf8.parquet"
OUTPUT_PATH = ROOT / "data" / "processed" / "candidates" / "01_ess_x_naf.csv"

CODES_NAF_NUMERIQUE = list(NAF_NUMERIQUE.keys())

# Colonnes minimales pour le filtrage + dénomination pour lisibilité
COLS_SIRENE = [
    "siren",
    "denominationUniteLegale",
    "activitePrincipaleUniteLegale",
]

# Métadonnées de traçabilité
SOURCES = [Source.ESS_FRANCE, Source.SIRENE]
METHODE = MethodeJointure.SIREN_DIRECT


def main():
    # --- 1. Chargement des SIREN ESS ---
    print("1. Chargement de la référence ESS...")
    ess_sirens = set(
        pd.read_csv(ESS_PATH, usecols=["SIREN"], dtype={"SIREN": str})["SIREN"]
    )
    print(f"   Structures ESS référencées : {len(ess_sirens):,}")

    # --- 2. Filtrage du stock INSEE par SIREN ESS + codes NAF ---
    print("\n2. Filtrage du stock Sirene (lecture par row groups)...")
    pf = pq.ParquetFile(SIRENE_PATH)
    num_rg = pf.metadata.num_row_groups
    print(f"   Row groups : {num_rg}, Lignes totales : {pf.metadata.num_rows:,}")

    results = []
    for i in range(num_rg):
        df_chunk = pf.read_row_group(i, columns=COLS_SIRENE).to_pandas()
        mask = (
            df_chunk["siren"].isin(ess_sirens)
            & df_chunk["activitePrincipaleUniteLegale"].isin(CODES_NAF_NUMERIQUE)
        )
        filtered = df_chunk[mask]
        if len(filtered) > 0:
            results.append(filtered)
        if (i + 1) % 50 == 0:
            print(f"   {i + 1}/{num_rg} row groups traités...")

    df = pd.concat(results, ignore_index=True)
    print(f"\n   {len(df):,} structures ESS + numérique identifiées")

    # --- 3. Ajout des métadonnées de traçabilité ---
    df["sources"] = "|".join(s.value for s in SOURCES)
    df["methode_jointure"] = METHODE.value

    # --- 4. Résultats ---
    print("\n3. Répartition par code NAF :")
    print(df["activitePrincipaleUniteLegale"].value_counts().to_string())

    # --- 5. Export ---
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_PATH, index=False)
    print(f"\n4. Export : {OUTPUT_PATH} ({len(df):,} lignes)")


if __name__ == "__main__":
    main()
