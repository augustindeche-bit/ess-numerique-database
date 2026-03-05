"""
Identification des tiers-lieux à activité numérique

Filtre la base France Tiers-Lieux (section 04 — Activités) sur les colonnes
caractérisant une activité numérique (fabrication numérique, inclusion,
médiation, formation informatique, équipement numérique…).

Jointure avec le fichier identité (section 01) pour récupérer le nom de la
structure porteuse, l'adresse et le site web.

La résolution SIREN est effectuée par matching de noms contre la base Sirene
(module src/02_DataEnrichment/resolve_siren_by_name.py).

Sources  : tiers_lieux
Jointure : SCORING (matching de noms contre Sirene)

Entrées :
    - data/raw/bdftl-2023-04-activites.csv      (activités)
    - data/raw/bdftl-2023-01-fiche-identite.csv  (identité, adresse)
    - data/raw/StockUniteLegale_utf8.parquet     (index Sirene)

Sortie :
    - data/processed/candidates/03_tiers_lieux.csv

Usage : python src/01_Candidates/03_tiers_lieux.py
"""

import sys
from pathlib import Path

import pandas as pd

_SRC = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_SRC))
sys.path.insert(0, str(_SRC / "02_DataEnrichment"))
from src.models import Source, MethodeJointure
from src.resolve_siren_by_name import build_sirene_index, resolve_siren_by_name

ROOT = Path(__file__).resolve().parents[2]
ACTIVITES_PATH = ROOT / "data" / "raw" / "bdftl-2023-04-activites.csv"
IDENTITE_PATH = ROOT / "data" / "raw" / "bdftl-2023-01-fiche-identite.csv"
OUTPUT_PATH = ROOT / "data" / "processed" / "candidates" / "03_tiers_lieux.csv"

# Colonnes caractérisant une activité numérique
COLS_NUMERIQUE = [
    "TYPE_ACT_NUMER",       # Activité numérique
    "PROD_NUMER",           # Production numérique
    "THEME_FORM_NUMER",     # Formation numérique
    "THEME_FORM_INFOR",     # Formation informatique
    "THEME_FORM_PROGRAM",   # Formation programmation
    "OFFRE_NUM_FABR",       # Fabrication numérique
    "OFFRE_NUM_INCLU",      # Inclusion numérique
    "OFFRE_NUM_MENUM",      # Médiation numérique
    "EQPT_NUM",             # Équipement numérique
]

# Colonnes à garder du fichier identité
COLS_IDENTITE = [
    "ID_UNIQUE", "NOM", "Insee", "NOM_STRC_PORT",
    "ADRESSE", "CODPOST", "VILLE", "INTERNET",
]

# Métadonnées de traçabilité
SOURCES = [Source.TIERS_LIEUX]
METHODE = MethodeJointure.SCORING  # pas de SIREN direct, résolution à faire


def main():
    # --- 1. Chargement des activités ---
    print("1./6 Chargement de la base Tiers-Lieux (activités)...")
    df_act = pd.read_csv(ACTIVITES_PATH, sep=";", low_memory=False)
    print(f"   {len(df_act):,} tiers-lieux au total")

    # --- 2. Filtrage sur les colonnes numériques ---
    print("\n2./6 Filtrage sur les colonnes numériques...")
    df_num = df_act[df_act[COLS_NUMERIQUE].all(axis=1)].copy()
    print(f"   {len(df_num):,} tiers-lieux avec toutes les caractéristiques numériques")

    # --- 3. Jointure avec le fichier identité ---
    print("\n3./6 Jointure avec le fichier identité...")
    df_id = pd.read_csv(IDENTITE_PATH, sep=";", usecols=COLS_IDENTITE, low_memory=False)
    df_merged = df_num[["ID_UNIQUE"]].merge(df_id, on="ID_UNIQUE", how="left")
    print(f"   {df_merged['NOM'].notna().sum()}/{len(df_merged)} noms résolus")

    # --- 4. Résolution SIREN par matching de noms ---
    print("\n4./6 Résolution SIREN par matching de noms contre Sirene...")
    sirene_index = build_sirene_index()
    df_resolved = resolve_siren_by_name(
        df_merged,
        sirene_index,
        name_col="NOM",
        alt_name_col="NOM_STRC_PORT",
    )

    # --- 5. Ajout des métadonnées de traçabilité ---
    print("\n5./6 Ajout des métadonnées de traçabilité...")
    df_resolved["sources"] = "|".join(s.value for s in SOURCES)
    # methode_jointure est déjà rempli par resolve_siren_by_name pour les résolus
    mask_no_method = df_resolved["methode_jointure"].isna()
    df_resolved.loc[mask_no_method, "methode_jointure"] = METHODE.value

    # --- 6. Export ---
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df_resolved.to_csv(OUTPUT_PATH, index=False)
    print(f"\n6./6 Export : {OUTPUT_PATH} ({len(df_resolved):,} lignes)")


if __name__ == "__main__":
    main()
