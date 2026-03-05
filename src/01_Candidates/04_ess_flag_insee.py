"""
Identification des structures ESS par flag INSEE + codes NAF numeriques.

Filtre la base Sirene par :
  - economieSocialeSolidaireUniteLegale == 'O' (flag ESS INSEE)
  - ET activitePrincipaleUniteLegale dans les 10 codes NAF numeriques

Source  : sirene (flag ESS + NAF)
Jointure : siren_direct

Entrees :
    - data/raw/StockUniteLegale_utf8.parquet  (Stock INSEE ~30M lignes)

Sortie :
    - data/processed/candidates/04_ess_flag_insee.csv

Usage : python src/01_Candidates/04_ess_flag_insee.py
"""

import sys
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from src.models import NAF_NUMERIQUE, Source, MethodeJointure

ROOT = Path(__file__).resolve().parents[2]
SIRENE_PATH = ROOT / "data" / "raw" / "StockUniteLegale_utf8.parquet"
OUTPUT_PATH = ROOT / "data" / "processed" / "candidates" / "04_ess_flag_insee.csv"

CODES_NAF_NUMERIQUE = list(NAF_NUMERIQUE.keys())

COLS_SIRENE = [
    "siren",
    "denominationUniteLegale",
    "nomUniteLegale",
    "activitePrincipaleUniteLegale",
    "economieSocialeSolidaireUniteLegale",
    "categorieJuridiqueUniteLegale",
    "trancheEffectifsUniteLegale",
]

SOURCES = [Source.SIRENE]
METHODE = MethodeJointure.SIREN_DIRECT


def main(sirene_path: Path | None = None, output_path: Path | None = None):
    sirene_path = sirene_path or SIRENE_PATH
    output_path = output_path or OUTPUT_PATH

    # --- 1. Filtrage du stock INSEE ---
    print("1./3 Filtrage du stock Sirene (lecture par row groups)...")
    print(f"   Critere : flag ESS INSEE == 'O' ET NAF dans {CODES_NAF_NUMERIQUE}")
    pf = pq.ParquetFile(sirene_path)
    num_rg = pf.metadata.num_row_groups
    print(f"   Row groups : {num_rg}, Lignes totales : {pf.metadata.num_rows:,}")

    results = []
    for i in range(num_rg):
        df_chunk = pf.read_row_group(i, columns=COLS_SIRENE).to_pandas()
        is_ess = df_chunk["economieSocialeSolidaireUniteLegale"] == "O"
        is_naf_num = df_chunk["activitePrincipaleUniteLegale"].isin(CODES_NAF_NUMERIQUE)
        mask = is_ess & is_naf_num
        filtered = df_chunk[mask]
        if len(filtered) > 0:
            results.append(filtered)
        if (i + 1) % 50 == 0:
            print(f"   {i + 1}/{num_rg} row groups traites...")

    df = pd.concat(results, ignore_index=True)
    df = df.dropna(subset=["activitePrincipaleUniteLegale"])
    print(f"\n   {len(df):,} structures identifiees")

    # --- 2. Ajout des metadonnees de tracabilite ---
    df["sources"] = "|".join(s.value for s in SOURCES)
    df["methode_jointure"] = METHODE.value

    # --- 3. Export ---
    print("\n2./3 Repartition par code NAF :")
    print(df["activitePrincipaleUniteLegale"].value_counts().to_string())

    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    print(f"\n3./3 Export : {output_path} ({len(df):,} lignes)")


if __name__ == "__main__":
    main()
