"""
Enrichissement depuis SIRENE (US B.2)

Enrichit les champs vides du fichier consolidé x_consolidated.csv
depuis le parquet SIRENE (StockUniteLegale_utf8.parquet, ~30M lignes).

Chaque champ enrichi reçoit deux colonnes de traçabilité :
  - <field>_enrich_source  = "sirene"
  - <field>_enrich_method  = "siren_direct"

Entrées :
    - data/processed/candidates/x_consolidated.csv
    - data/raw/StockUniteLegale_utf8.parquet

Sortie :
    - data/processed/candidates/x_enriched.csv

Usage : python src/02_DataEnrichment/enrich_from_sirene.py
"""

import sys
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from src.models import (
    NAF_NUMERIQUE,
    CATEGORIE_JURIDIQUE_FAMILLE,
)

ROOT = Path(__file__).resolve().parents[2]
CONSOLIDATED_PATH = ROOT / "data" / "processed" / "candidates" / "x_consolidated.csv"
SIRENE_PATH = ROOT / "data" / "raw" / "StockUniteLegale_utf8.parquet"
OUTPUT_PATH = ROOT / "data" / "processed" / "candidates" / "x_enriched.csv"

# ──────────────────────────────────────────────
# Mapping SIRENE → modèle consolidé
# ──────────────────────────────────────────────

SIRENE_FIELD_MAPPING = [
    # (sirene_col, target_col, transform)
    ("activitePrincipaleUniteLegale", "naf", None),
    ("categorieJuridiqueUniteLegale", "categorie_juridique", None),
    ("trancheEffectifsUniteLegale", "tranche_effectifs", None),
    ("denominationUniteLegale", "denomination", None),
    ("nomUniteLegale", "nom_usage", None),
    ("sigleUniteLegale", "sigle", None),
    ("dateCreationUniteLegale", "date_creation", None),
    ("etatAdministratifUniteLegale", "est_active", "est_active"),
    ("identifiantAssociationUniteLegale", "rna", None),
    ("economieSocialeSolidaireUniteLegale", "flag_ess_sirene", "flag_ess"),
    ("caractereEmployeurUniteLegale", "caractere_employeur", None),
    ("categorieEntreprise", "categorie_entreprise", None),
    ("societeMissionUniteLegale", "societe_mission", None),
    ("anneeEffectifsUniteLegale", "annee_effectifs", None),
]

# Colonnes SIRENE nécessaires pour le chargement
SIRENE_COLS = (
    ["siren", "dateDebut", "nicSiegeUniteLegale"]
    + [col for col, _, _ in SIRENE_FIELD_MAPPING]
)


# ──────────────────────────────────────────────
# Transforms
# ──────────────────────────────────────────────


def _transform_est_active(val):
    """'A' → True, 'C' → False, sinon None."""
    if pd.isna(val):
        return None
    s = str(val).strip()
    if s == "A":
        return True
    if s == "C":
        return False
    return None


def _transform_flag_ess(val):
    """'O' → True, sinon None."""
    if pd.isna(val):
        return None
    return True if str(val).strip() == "O" else None


TRANSFORMS = {
    "est_active": _transform_est_active,
    "flag_ess": _transform_flag_ess,
}


# ──────────────────────────────────────────────
# Chargement SIRENE filtré
# ──────────────────────────────────────────────


def load_sirene_for_sirens(
    sirens: set[str], parquet_path: Path
) -> pd.DataFrame:
    """Charge le parquet SIRENE filtré sur un ensemble de SIRENs.

    Lecture par row-group pour gérer le fichier de ~30M lignes.
    Dédupliqué par dateDebut desc pour garder la période la plus récente.
    """
    pf = pq.ParquetFile(parquet_path)
    num_rg = pf.metadata.num_row_groups

    chunks = []
    for i in range(num_rg):
        df_chunk = pf.read_row_group(i, columns=SIRENE_COLS).to_pandas()
        filtered = df_chunk[df_chunk["siren"].isin(sirens)]
        if len(filtered) > 0:
            chunks.append(filtered)

    if not chunks:
        return pd.DataFrame(columns=SIRENE_COLS)

    df = pd.concat(chunks, ignore_index=True)

    # Dédupliquer : garder la période la plus récente par SIREN
    df["dateDebut"] = pd.to_datetime(df["dateDebut"], errors="coerce")
    df = df.sort_values("dateDebut", ascending=False).drop_duplicates(
        subset=["siren"], keep="first"
    )

    return df.set_index("siren")


# ──────────────────────────────────────────────
# Enrichissement générique
# ──────────────────────────────────────────────


def enrich_field(
    df: pd.DataFrame,
    lookup: pd.DataFrame,
    sirene_col: str,
    target_col: str,
    transform: str | None = None,
) -> pd.DataFrame:
    """Enrichit un champ vide depuis SIRENE avec traçabilité.

    Pour chaque ligne où target_col est vide/NaN :
      - Copie la valeur depuis lookup[sirene_col] (via siren)
      - Applique un transform si spécifié
      - Ajoute <target_col>_enrich_source = "sirene"
      - Ajoute <target_col>_enrich_method = "siren_direct"
    """
    source_col = f"{target_col}_enrich_source"
    method_col = f"{target_col}_enrich_method"

    # Initialiser les colonnes de traçabilité si absentes
    if source_col not in df.columns:
        df[source_col] = None
    if method_col not in df.columns:
        df[method_col] = None

    # Assurer que la colonne cible existe (type object pour éviter FutureWarning)
    if target_col not in df.columns:
        df[target_col] = pd.Series(dtype="object", index=df.index)
    elif df[target_col].dtype != "object":
        df[target_col] = df[target_col].astype("object")

    # Identifier les lignes vides
    is_empty = df[target_col].isna()
    if target_col not in ("est_active", "flag_ess_sirene"):
        # Pour les champs texte, considérer "" comme vide aussi
        is_empty = is_empty | (df[target_col].astype(str).str.strip() == "")

    # Mapper les valeurs SIRENE via siren
    if sirene_col not in lookup.columns:
        return df

    sirene_vals = df["siren"].map(lookup[sirene_col])

    # Appliquer le transform si nécessaire
    if transform and transform in TRANSFORMS:
        sirene_vals = sirene_vals.map(TRANSFORMS[transform])

    # Ne remplir que là où : ligne vide ET valeur SIRENE disponible
    has_sirene = sirene_vals.notna()
    if transform is None:
        # Pour les champs sans transform, exclure aussi les chaînes vides
        has_sirene = has_sirene & (sirene_vals.astype(str).str.strip() != "")

    mask = is_empty & has_sirene

    df.loc[mask, target_col] = sirene_vals[mask]
    df.loc[mask, source_col] = "sirene"
    df.loc[mask, method_col] = "siren_direct"

    return df


# ──────────────────────────────────────────────
# SIRET siège
# ──────────────────────────────────────────────


def compute_siret_siege(
    df: pd.DataFrame, lookup: pd.DataFrame
) -> pd.DataFrame:
    """Calcule siret_siege = siren + nicSiegeUniteLegale (14 chiffres).

    Ne remplit que si siret_siege est vide et le NIC est valide (5 chiffres).
    """
    source_col = "siret_siege_enrich_source"
    method_col = "siret_siege_enrich_method"

    if "siret_siege" not in df.columns:
        df["siret_siege"] = None
    if source_col not in df.columns:
        df[source_col] = None
    if method_col not in df.columns:
        df[method_col] = None

    if "nicSiegeUniteLegale" not in lookup.columns:
        return df

    is_empty = df["siret_siege"].isna() | (
        df["siret_siege"].astype(str).str.strip() == ""
    )

    nic_vals = df["siren"].map(lookup["nicSiegeUniteLegale"])
    nic_str = nic_vals.astype(str).str.strip()

    # NIC valide : 5 chiffres
    nic_valid = nic_str.str.match(r"^\d{5}$", na=False)

    mask = is_empty & nic_valid
    siret = df.loc[mask, "siren"] + nic_str[mask]

    # Valider que le SIRET fait bien 14 chiffres
    siret_valid = siret.str.match(r"^\d{14}$", na=False)
    mask_final = mask & siret_valid.reindex(mask.index, fill_value=False)

    df.loc[mask_final, "siret_siege"] = (
        df.loc[mask_final, "siren"] + nic_str[mask_final]
    )
    df.loc[mask_final, source_col] = "sirene"
    df.loc[mask_final, method_col] = "siren_direct"

    return df


# ──────────────────────────────────────────────
# Re-dérivation post-enrichissement
# ──────────────────────────────────────────────


def re_derive_from_enriched(df: pd.DataFrame) -> pd.DataFrame:
    """Re-dérive famille_ess, naf_numerique, tags_numerique
    là où naf ou categorie_juridique viennent d'être enrichis."""

    # Re-dériver famille_ess là où categorie_juridique a été enrichie
    if "categorie_juridique_enrich_source" not in df.columns:
        cj_enriched = pd.Series(False, index=df.index)
    else:
        cj_enriched = df["categorie_juridique_enrich_source"] == "sirene"
    if cj_enriched.any():
        def _map_famille(cj):
            if pd.isna(cj):
                return None
            try:
                key = str(int(float(cj)))
            except (ValueError, TypeError):
                return None
            fam = CATEGORIE_JURIDIQUE_FAMILLE.get(key)
            return fam.value if fam else None

        if "famille_ess" in df.columns and df["famille_ess"].dtype != "object":
            df["famille_ess"] = df["famille_ess"].astype("object")
        df.loc[cj_enriched, "famille_ess"] = (
            df.loc[cj_enriched, "categorie_juridique"].map(_map_famille)
        )

    # Re-dériver naf_numerique et tags_numerique là où naf a été enrichi
    if "naf_enrich_source" not in df.columns:
        naf_enriched = pd.Series(False, index=df.index)
    else:
        naf_enriched = df["naf_enrich_source"] == "sirene"
    if naf_enriched.any():
        df.loc[naf_enriched, "naf_numerique"] = df.loc[
            naf_enriched, "naf"
        ].isin(NAF_NUMERIQUE.keys())
        if "tags_numerique" in df.columns and df["tags_numerique"].dtype != "object":
            df["tags_numerique"] = df["tags_numerique"].astype("object")
        df.loc[naf_enriched, "tags_numerique"] = df.loc[
            naf_enriched, "naf"
        ].map(
            lambda n: NAF_NUMERIQUE[n].value if n in NAF_NUMERIQUE else None
        )

    return df


# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────


def main(
    consolidated_path: Path = CONSOLIDATED_PATH,
    sirene_path: Path = SIRENE_PATH,
    output_path: Path = OUTPUT_PATH,
):
    print("=" * 60)
    print("Enrichissement depuis SIRENE (US B.2)")
    print("=" * 60)

    # --- 1. Charger le fichier consolidé ---
    print("\n1. Chargement du fichier consolide...")
    df = pd.read_csv(consolidated_path, dtype={"siren": str})
    print(f"   {len(df):,} lignes, {len(df.columns)} colonnes")

    # --- 2. Charger SIRENE filtré ---
    sirens = set(df["siren"].dropna())
    print(f"\n2. Chargement SIRENE pour {len(sirens):,} SIRENs...")
    lookup = load_sirene_for_sirens(sirens, sirene_path)
    print(f"   {len(lookup):,} lignes SIRENE chargees")

    # --- 3. Enrichissement champ par champ ---
    print("\n3. Enrichissement des champs vides...")
    enriched_counts = {}
    for sirene_col, target_col, transform in SIRENE_FIELD_MAPPING:
        n_before = df[target_col].notna().sum() if target_col in df.columns else 0
        df = enrich_field(df, lookup, sirene_col, target_col, transform)
        n_after = df[target_col].notna().sum()
        n_enriched = n_after - n_before
        if n_enriched > 0:
            enriched_counts[target_col] = n_enriched
            print(f"   {target_col:25s} : +{n_enriched:,} valeurs")

    # --- 4. SIRET siège ---
    print("\n4. Calcul SIRET siege...")
    n_before = df["siret_siege"].notna().sum() if "siret_siege" in df.columns else 0
    df = compute_siret_siege(df, lookup)
    n_after = df["siret_siege"].notna().sum()
    print(f"   siret_siege             : +{n_after - n_before:,} valeurs")

    # --- 5. Re-dérivation ---
    print("\n5. Re-derivation famille_ess / naf_numerique / tags_numerique...")
    df = re_derive_from_enriched(df)

    # --- 6. Rapport ---
    total_enriched = sum(enriched_counts.values()) + (n_after - n_before)
    print(f"\n6. Rapport :")
    print(f"   Champs enrichis au total : {total_enriched:,} valeurs")
    print(f"   Colonnes de sortie       : {len(df.columns)}")

    # --- 7. Export ---
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False, encoding="utf-8-sig")
    print(f"\n7. Export : {output_path} ({len(df):,} lignes)")

    return df


if __name__ == "__main__":
    main()
