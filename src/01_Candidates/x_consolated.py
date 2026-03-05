"""
Consolidation des fichiers candidats par SIREN.

Fusionne les 4 fichiers produits par les scripts 01_Candidates :
  - 01_ess_x_naf.csv       (ESS France × NAF numérique)
  - 02_cate_x_naf.csv      (catégorie juridique / ESUS × NAF)
  - 03_tiers_lieux.csv     (tiers-lieux à activité numérique)
  - 04_ess_flag_insee.csv  (flag ESS INSEE × NAF numérique)

Clé unique : SIREN
Règles de fusion :
  - Attributs concordants entre sources → valeur unique
  - Attributs divergents  → colonnes suffixées par origine
    (ex : denomination__ess_x_naf, denomination__tiers_lieux)
  - Sources agrégées (union)

Sortie : data/processed/candidates/x_consolidated.csv
Usage  : python src/01_Candidates/x_consolated.py
"""

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from src.models import (
    NAF_NUMERIQUE,
    CATEGORIE_JURIDIQUE_FAMILLE,
    Source,
    MethodeJointure,
)

ROOT = Path(__file__).resolve().parents[2]
CANDIDATES_DIR = ROOT / "data" / "processed" / "candidates"
OUTPUT_PATH = CANDIDATES_DIR / "x_consolidated.csv"

# ──────────────────────────────────────────────
# Configuration des fichiers d'entrée
# ──────────────────────────────────────────────

INPUTS = {
    "ess_x_naf": {
        "path": CANDIDATES_DIR / "01_ess_x_naf.csv",
        "rename": {
            "denominationUniteLegale": "denomination",
            "activitePrincipaleUniteLegale": "naf",
        },
        "defaults": {},
    },
    "cate_x_naf": {
        "path": CANDIDATES_DIR / "02_cate_x_naf.csv",
        "rename": {
            "denominationUniteLegale": "denomination",
            "nomUniteLegale": "nom_usage",
            "activitePrincipaleUniteLegale": "naf",
            "trancheEffectifsUniteLegale": "tranche_effectifs",
            "categorieJuridiqueUniteLegale": "categorie_juridique",
        },
        "defaults": {},
    },
    "tiers_lieux": {
        "path": CANDIDATES_DIR / "03_tiers_lieux.csv",
        "rename": {
            "NOM": "denomination",
            "NOM_STRC_PORT": "nom_structure_porteuse",
            "CODPOST": "code_postal",
            "VILLE": "commune",
            "INTERNET": "site_web",
        },
        "defaults": {},
    },
    "ess_flag_insee": {
        "path": CANDIDATES_DIR / "04_ess_flag_insee.csv",
        "rename": {
            "denominationUniteLegale": "denomination",
            "nomUniteLegale": "nom_usage",
            "activitePrincipaleUniteLegale": "naf",
            "categorieJuridiqueUniteLegale": "categorie_juridique",
            "trancheEffectifsUniteLegale": "tranche_effectifs",
        },
        "defaults": {},
    },
}

# Attributs du modèle OrganisationESS susceptibles de varier entre sources
ATTRS_MERGEABLE = [
    "denomination",
    "nom_usage",
    "naf",
    "code_postal",
    "commune",
    "site_web",
    "tranche_effectifs",
    "categorie_juridique",
]

# Priorité des méthodes de jointure (plus fiable en premier)
METHODE_PRIORITY = [m.value for m in MethodeJointure]


# ──────────────────────────────────────────────
# Chargement & normalisation
# ──────────────────────────────────────────────


def load_and_normalize(origin: str, config: dict) -> pd.DataFrame:
    """Charge un CSV, renomme les colonnes, ajoute _origin."""
    path = config["path"]
    if not path.exists():
        print(f"   SKIP fichier absent : {path}")
        return pd.DataFrame()

    df = pd.read_csv(path, dtype={"siren": str})

    # Valeurs par défaut pour les colonnes manquantes
    for col, val in config["defaults"].items():
        if col not in df.columns:
            df[col] = val

    # Renommer les colonnes
    rename = {k: v for k, v in config["rename"].items() if k in df.columns}
    df = df.rename(columns=rename)

    df["_origin"] = origin

    # Normaliser SIREN : string 9 chiffres
    df["siren"] = df["siren"].astype(str).str.strip().str.split(".").str[0].str.zfill(9)

    # Garder uniquement les SIREN valides
    mask_valid = df["siren"].str.match(r"^\d{9}$", na=False)
    n_dropped = (~mask_valid).sum()
    df = df[mask_valid]

    if n_dropped > 0:
        print(f"   {origin}: {n_dropped} lignes sans SIREN valide ignorees")
    print(f"   {origin}: {len(df):,} lignes chargees depuis {path.name}")
    return df


# ──────────────────────────────────────────────
# Fusion par SIREN
# ──────────────────────────────────────────────


def merge_sources(group: pd.DataFrame) -> str:
    """Union de toutes les sources présentes dans le groupe."""
    all_sources = set()
    if "sources" in group.columns:
        for val in group["sources"].dropna():
            all_sources.update(str(val).split("|"))
    return "|".join(sorted(all_sources)) if all_sources else ""


def resolve_attribute(group: pd.DataFrame, attr: str) -> dict:
    """
    Résout un attribut pour un groupe SIREN.

    - Valeurs concordantes → {attr: valeur}
    - Valeurs divergentes  → {attr: val_principale,
                               attr__origin1: val1, attr__origin2: val2}
    """
    if attr not in group.columns:
        return {}

    pairs = []
    for _, row in group.iterrows():
        val = row.get(attr)
        if pd.notna(val) and str(val).strip():
            pairs.append((row["_origin"], str(val).strip()))

    if not pairs:
        return {}

    unique_vals = set(v for _, v in pairs)

    if len(unique_vals) == 1:
        return {attr: pairs[0][1]}

    # Divergence : valeur principale = première + versions par source
    result = {attr: pairs[0][1]}
    for origin, val in pairs:
        result[f"{attr}__{origin}"] = val
    return result


def best_methode(group: pd.DataFrame) -> str | None:
    """Retourne la méthode de jointure la plus fiable du groupe."""
    if "methode_jointure" not in group.columns:
        return None
    methods = group["methode_jointure"].dropna().unique()
    if len(methods) == 0:
        return None
    return min(
        methods,
        key=lambda m: METHODE_PRIORITY.index(m) if m in METHODE_PRIORITY else 99,
    )


def consolidate_group(siren: str, group: pd.DataFrame) -> dict:
    """Construit un enregistrement consolidé pour un SIREN."""
    record = {"siren": siren}

    # Sources (union)
    record["sources"] = merge_sources(group)

    # Origines (quels fichiers contribuent)
    record["origines"] = "|".join(sorted(group["_origin"].unique()))

    # Méthode de jointure (la plus fiable)
    methode = best_methode(group)
    if methode:
        record["methode_jointure"] = methode

    # Confiance jointure : max explicite, ou 1.0 si siren_direct
    if "confiance_jointure" in group.columns:
        conf = group["confiance_jointure"].dropna()
        if len(conf) > 0:
            record["confiance_jointure"] = conf.max()
    if "confiance_jointure" not in record and methode == MethodeJointure.SIREN_DIRECT.value:
        record["confiance_jointure"] = 1.0

    # Attributs fusionnés avec détection de conflits
    for attr in ATTRS_MERGEABLE:
        record.update(resolve_attribute(group, attr))

    # Colonnes supplémentaires (non fusionnées, premier non-nul)
    for col in ["nom_structure_porteuse", "group_naf", "group_cate"]:
        if col in group.columns:
            val = group[col].dropna()
            if len(val) > 0:
                record[col] = val.iloc[0]

    return record


# ──────────────────────────────────────────────
# Enrichissement
# ──────────────────────────────────────────────


def enrich(df: pd.DataFrame) -> pd.DataFrame:
    """Ajoute famille_ess, naf_numerique, tags_numerique depuis le modèle."""

    def map_famille(cj):
        if pd.isna(cj):
            return None
        key = str(int(float(cj)))
        fam = CATEGORIE_JURIDIQUE_FAMILLE.get(key)
        return fam.value if fam else None

    df["famille_ess"] = df["categorie_juridique"].map(map_famille)
    df["naf_numerique"] = df["naf"].isin(NAF_NUMERIQUE.keys())
    df["tags_numerique"] = df["naf"].map(
        lambda n: NAF_NUMERIQUE[n].value if n in NAF_NUMERIQUE else None
    )
    return df


# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────


def main():
    print("=" * 60)
    print("Consolidation des candidats ESS numerique")
    print("=" * 60)

    # --- 1. Chargement ---
    print("\n1. Chargement des fichiers...")
    frames = []
    for origin, config in INPUTS.items():
        df = load_and_normalize(origin, config)
        if not df.empty:
            frames.append(df)

    if not frames:
        print("Aucun fichier a consolider.")
        return

    # --- 2. Concaténation ---
    print(f"\n2. Concatenation...")
    df_all = pd.concat(frames, ignore_index=True)
    n_total = len(df_all)
    n_unique = df_all["siren"].nunique()
    print(f"   {n_total:,} lignes, {n_unique:,} SIREN uniques")

    # --- 3. Consolidation par SIREN ---
    print(f"\n3. Consolidation par SIREN...")
    records = []
    conflicts = {attr: 0 for attr in ATTRS_MERGEABLE}

    for siren, group in df_all.groupby("siren"):
        record = consolidate_group(siren, group)
        records.append(record)

        # Compteur de conflits
        for attr in ATTRS_MERGEABLE:
            if any(k.startswith(f"{attr}__") for k in record):
                conflicts[attr] += 1

    df_out = pd.DataFrame(records)

    # --- 4. Enrichissement ---
    print(f"\n4. Enrichissement (famille ESS, tags numerique)...")
    df_out = enrich(df_out)

    # --- 5. Rapport ---
    print(f"\n5. Rapport de consolidation :")
    print(f"   SIREN uniques       : {len(df_out):,}")
    print(f"   Colonnes de sortie  : {len(df_out.columns)}")
    for attr, count in conflicts.items():
        if count > 0:
            print(f"   Conflits {attr:20s} : {count:,} SIREN")

    n_multi = df_out["origines"].str.contains(r"\|").sum()
    print(f"   SIREN multi-sources : {n_multi:,}")

    print(f"\n   Repartition par origine :")
    for origin in INPUTS:
        n = df_out["origines"].str.contains(origin).sum()
        print(f"     {origin:20s} : {n:,}")

    # --- 6. Export ---
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df_out.to_csv(OUTPUT_PATH, index=False, encoding="utf-8-sig")
    print(f"\n6. Export : {OUTPUT_PATH} ({len(df_out):,} lignes)")


if __name__ == "__main__":
    main()
