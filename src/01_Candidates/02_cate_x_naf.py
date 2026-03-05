"""
Approche complémentaire : filtrer par forme juridique ESS ou agrément ESUS
+ codes NAF numériques

Filtre la base Sirene par :
  - catégorie juridique ESS (associations, coopératives, mutuelles, fondations)
  - OU agrément ESUS (liste nationale du Trésor)
croisée avec 10 codes NAF numériques.

Sources  : sirene (catégorie juridique + NAF) + esus (agrément)
Jointure : siren_direct — (catégorie juridique ESS ∪ SIREN ESUS) ∩ NAF numériques

Entrées :
    - data/raw/StockUniteLegale_utf8.parquet  (Stock INSEE ~30M lignes)
    - data/raw/liste_nationale_esus.xlsx      (Liste ESUS ~quelques milliers)

Sortie :
    - data/processed/candidates/02_cate_x_naf.csv

Usage : python src/01_Candidates/02_cate_x_naf.py
"""

import sys
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from src.models import CATEGORIE_JURIDIQUE_FAMILLE, NAF_NUMERIQUE, GROUP_NAF, GROUP_CATE, Source, MethodeJointure

ROOT = Path(__file__).resolve().parents[2]
SIRENE_PATH = ROOT / "data" / "raw" / "StockUniteLegale_utf8.parquet"
ESUS_PATH = ROOT / "data" / "raw" / "liste_nationale_esus.xlsx"
OUTPUT_PATH = ROOT / "data" / "processed" / "candidates" / "02_cate_x_naf.csv"

CODES_CATE_JURIDIQUE = [int(c) for c in CATEGORIE_JURIDIQUE_FAMILLE.keys()]

CODES_NAF_NUMERIQUE = list(NAF_NUMERIQUE.keys())

# Colonnes minimales pour le filtrage
COLS_SIRENE = [
    "siren",
    "denominationUniteLegale",
    "nomUniteLegale",
    "activitePrincipaleUniteLegale",
    "trancheEffectifsUniteLegale",
    "categorieJuridiqueUniteLegale",
]

# Métadonnées de traçabilité
SOURCES_CATE = [Source.SIRENE]
SOURCES_ESUS = [Source.SIRENE, Source.ESUS]
METHODE = MethodeJointure.SIREN_DIRECT


def load_esus_sirens() -> set[str]:
    """Charge les SIREN depuis la liste nationale ESUS."""
    df = pd.read_excel(ESUS_PATH)
    # Cherche la colonne contenant les SIREN (nom variable selon les versions)
    siren_col = None
    for col in df.columns:
        if "siren" in col.lower():
            siren_col = col
            break
    if siren_col is None:
        # Fallback : première colonne numérique à 9 chiffres
        for col in df.columns:
            sample = df[col].dropna().astype(str).str.strip()
            if sample.str.match(r"^\d{9}$").mean() > 0.5:
                siren_col = col
                break
    if siren_col is None:
        raise ValueError(f"Colonne SIREN introuvable dans {ESUS_PATH}. Colonnes : {list(df.columns)}")
    return set(df[siren_col].dropna().astype(str).str.strip().str.zfill(9))


def main():
    # --- 1. Chargement des SIREN ESUS ---
    print("1./4 Chargement de la liste ESUS...")
    esus_sirens = load_esus_sirens()
    print(f"   Structures ESUS référencées : {len(esus_sirens):,}")

    # --- 2. Filtrage du stock INSEE ---
    print("\n2./4 Filtrage du stock Sirene (lecture par row groups)...")
    print(f"   Critère ESS : catégorie juridique {CODES_CATE_JURIDIQUE} OU SIREN ESUS")
    print(f"   Codes NAF ciblés : {CODES_NAF_NUMERIQUE}")
    pf = pq.ParquetFile(SIRENE_PATH)
    num_rg = pf.metadata.num_row_groups
    print(f"   Row groups : {num_rg}, Lignes totales : {pf.metadata.num_rows:,}")

    results = []
    for i in range(num_rg):
        df_chunk = pf.read_row_group(i, columns=COLS_SIRENE).to_pandas()
        is_naf_num = df_chunk["activitePrincipaleUniteLegale"].isin(CODES_NAF_NUMERIQUE)
        is_cate_ess = df_chunk["categorieJuridiqueUniteLegale"].isin(CODES_CATE_JURIDIQUE)
        is_esus = df_chunk["siren"].isin(esus_sirens)
        mask = is_naf_num & (is_cate_ess | is_esus)
        filtered = df_chunk[mask]
        if len(filtered) > 0:
            results.append(filtered)
        if (i + 1) % 50 == 0:
            print(f"   {i + 1}/{num_rg} row groups traités...")

    df = pd.concat(results, ignore_index=True)
    df = df.dropna(subset=["activitePrincipaleUniteLegale"])
    print(f"\n   {len(df):,} structures identifiées")

    # --- 3. Colonnes de groupage ---
    df["group_naf"] = df["activitePrincipaleUniteLegale"].map(GROUP_NAF)
    df["group_cate"] = df["categorieJuridiqueUniteLegale"].astype(str).str.split(".").str[0].map(GROUP_CATE)

    # --- 4. Ajout des métadonnées de traçabilité ---
    is_cate = df["categorieJuridiqueUniteLegale"].isin(CODES_CATE_JURIDIQUE)
    is_esus = df["siren"].isin(esus_sirens)

    def build_sources(row_cate, row_esus):
        sources = {Source.SIRENE}
        if row_cate:
            sources.add(Source.SIRENE)  # critère catégorie juridique via Sirene
        if row_esus:
            sources.add(Source.ESUS)
        return "|".join(sorted(s.value for s in sources))

    df["sources"] = [build_sources(c, e) for c, e in zip(is_cate, is_esus)]
    df["methode_jointure"] = METHODE.value

    n_cate = is_cate.sum()
    n_esus_count = is_esus.sum()
    n_both = (is_cate & is_esus).sum()
    print(f"   {n_cate:,} via catégorie juridique, {n_esus_count:,} via ESUS, {n_both:,} via les deux")

    # --- 5. Résultats ---
    print("\n4./5 Répartition par code NAF :")
    print(df["activitePrincipaleUniteLegale"].value_counts().to_string())
    print("\n   Répartition par catégorie juridique :")
    print(df["categorieJuridiqueUniteLegale"].value_counts().to_string())

    # --- 6. Export ---
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_PATH, index=False)
    print(f"\n5./5 Export : {OUTPUT_PATH} ({len(df):,} lignes)")


if __name__ == "__main__":
    main()
