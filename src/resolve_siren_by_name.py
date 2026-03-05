"""
Résolution SIREN par matching de noms contre la base Sirene

Module générique : fournit ``build_sirene_index`` et ``resolve_siren_by_name``
réutilisables par tout script candidat dont les lignes n'ont pas de SIREN.

Trois passes par confiance décroissante :
  1. Match exact normalisé sur ``name_col``        → confiance 1.0
  2. Match exact sur ``alt_name_col`` (optionnel)   → confiance 0.9
  3. Match fuzzy (token_sort_ratio ≥ 80)            → confiance score/100
"""

from __future__ import annotations

import re
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
from text_unidecode import unidecode

try:
    from rapidfuzz import fuzz, process as rf_process
    HAS_RAPIDFUZZ = True
except ImportError:
    from difflib import SequenceMatcher
    HAS_RAPIDFUZZ = False

# ── Defaults ───────────────────────────────────────────────────────────

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SIRENE_PATH = ROOT / "data" / "raw" / "StockUniteLegale_utf8.parquet"

SIRENE_COLS = [
    "siren",
    "denominationUniteLegale",
    "sigleUniteLegale",
    "etatAdministratifUniteLegale",
]

FUZZY_THRESHOLD = 80

# ── Normalisation ──────────────────────────────────────────────────────

_RE_NON_ALNUM = re.compile(r"[^a-z0-9 ]")
_RE_MULTI_SPACE = re.compile(r"\s+")


def normalize_name(name: str | None) -> str:
    """Minuscules, suppression accents/ponctuation, espaces multiples."""
    if not name or not isinstance(name, str):
        return ""
    s = unidecode(name).lower()
    s = _RE_NON_ALNUM.sub(" ", s)
    s = _RE_MULTI_SPACE.sub(" ", s).strip()
    return s


# ── Build Sirene index ─────────────────────────────────────────────────

def build_sirene_index(
    parquet_path: Path = DEFAULT_SIRENE_PATH,
) -> dict[str, str]:
    """Charge le parquet Sirene par row groups → dict {nom_normalisé: siren}.

    Seules les entités actives avec une dénomination non-nulle sont indexées.
    Le sigle est ajouté comme clé alternative.
    """
    pf = pq.ParquetFile(parquet_path)
    index: dict[str, str] = {}
    n_groups = pf.num_row_groups

    print(f"   Chargement Sirene : {pf.metadata.num_rows:,} lignes ({n_groups} row groups)")

    for i in range(n_groups):
        table = pf.read_row_group(i, columns=SIRENE_COLS)
        df = table.to_pandas()
        df = df[df["etatAdministratifUniteLegale"] == "A"]

        for _, row in df.iterrows():
            siren = row["siren"]
            denom = row["denominationUniteLegale"]
            sigle = row["sigleUniteLegale"]

            if pd.notna(denom):
                norm = normalize_name(denom)
                if norm and norm not in index:
                    index[norm] = siren

            if pd.notna(sigle):
                norm_s = normalize_name(sigle)
                if norm_s and norm_s not in index:
                    index[norm_s] = siren

        if (i + 1) % 50 == 0 or i == n_groups - 1:
            print(f"   ... {i + 1}/{n_groups} row groups  ({len(index):,} noms indexés)")

    return index


# ── Resolve ────────────────────────────────────────────────────────────

def resolve_siren_by_name(
    df: pd.DataFrame,
    sirene_index: dict[str, str],
    name_col: str = "NOM",
    alt_name_col: str | None = "NOM_STRC_PORT",
    fuzzy: bool = True,
) -> pd.DataFrame:
    """Résout le SIREN par matching de noms (3 passes).

    Parameters
    ----------
    df : DataFrame candidat (sera copié, pas modifié en place).
    sirene_index : dict {nom_normalisé: siren} construit par ``build_sirene_index``.
    name_col : colonne du nom principal à matcher.
    alt_name_col : colonne du nom alternatif (passe 2). None pour ignorer.
    fuzzy : activer la passe 3 (fuzzy matching).

    Returns
    -------
    DataFrame enrichi avec colonnes ``siren``, ``confiance_jointure``,
    ``methode_jointure``.
    """
    out = df.copy()
    out["siren"] = None
    out["confiance_jointure"] = None
    out["methode_jointure"] = None
    n = len(out)

    # ── Passe 1 : match exact sur name_col ─────────────────────────────
    print(f"\n   Passe 1 — match exact sur «{name_col}»")
    p1 = 0
    for idx in out.index:
        norm = normalize_name(out.at[idx, name_col])
        if norm and norm in sirene_index:
            out.at[idx, "siren"] = sirene_index[norm]
            out.at[idx, "confiance_jointure"] = 1.0
            out.at[idx, "methode_jointure"] = "scoring"
            p1 += 1
    print(f"   → {p1}/{n} résolus")

    # ── Passe 2 : match exact sur alt_name_col ─────────────────────────
    has_alt = alt_name_col and alt_name_col in out.columns
    p2 = 0
    if has_alt:
        print(f"\n   Passe 2 — match exact sur «{alt_name_col}»")
        mask = out["siren"].isna()
        for idx in out[mask].index:
            norm = normalize_name(out.at[idx, alt_name_col])
            if norm and norm in sirene_index:
                out.at[idx, "siren"] = sirene_index[norm]
                out.at[idx, "confiance_jointure"] = 0.9
                out.at[idx, "methode_jointure"] = "scoring"
                p2 += 1
        print(f"   → {p2}/{mask.sum()} résolus")
    else:
        print(f"\n   Passe 2 — ignorée (colonne «{alt_name_col}» absente)")

    # ── Passe 3 : fuzzy matching ───────────────────────────────────────
    p3 = 0
    mask = out["siren"].isna()
    unresolved = mask.sum()

    if not fuzzy or unresolved == 0:
        if unresolved == 0:
            print("\n   Passe 3 — tous déjà résolus, fuzzy non nécessaire")
        else:
            print("\n   Passe 3 — fuzzy désactivé")
    else:
        print(f"\n   Passe 3 — fuzzy (seuil ≥ {FUZZY_THRESHOLD})")
        sirene_names = list(sirene_index.keys())
        engine = "rapidfuzz" if HAS_RAPIDFUZZ else "difflib (fallback)"
        print(f"   engine={engine}, pool={len(sirene_names):,} noms")

        for idx in out[mask].index:
            queries = []
            norm_nom = normalize_name(out.at[idx, name_col])
            if norm_nom:
                queries.append(norm_nom)
            if has_alt:
                norm_alt = normalize_name(out.at[idx, alt_name_col])
                if norm_alt and norm_alt != norm_nom:
                    queries.append(norm_alt)

            best_score = 0.0
            best_siren = None

            for query in queries:
                if HAS_RAPIDFUZZ:
                    result = rf_process.extractOne(
                        query,
                        sirene_names,
                        scorer=fuzz.token_sort_ratio,
                        score_cutoff=FUZZY_THRESHOLD,
                    )
                    if result:
                        match_name, score, _ = result
                        if score > best_score:
                            best_score = score
                            best_siren = sirene_index[match_name]
                else:
                    from difflib import SequenceMatcher
                    for sn in sirene_names[:500_000]:
                        score = SequenceMatcher(None, query, sn).ratio() * 100
                        if score > best_score:
                            best_score = score
                            best_siren = sirene_index[sn]

            if best_siren and best_score >= FUZZY_THRESHOLD:
                out.at[idx, "siren"] = best_siren
                out.at[idx, "confiance_jointure"] = round(best_score / 100, 2)
                out.at[idx, "methode_jointure"] = "scoring"
                p3 += 1
                print(f"     fuzzy {best_score:.0f}% : «{out.at[idx, name_col]}» → {best_siren}")

        print(f"   → {p3}/{unresolved} résolus par fuzzy")

    # ── Résumé ─────────────────────────────────────────────────────────
    total = out["siren"].notna().sum()
    print(f"\n   Résumé : {total}/{n} résolus (P1={p1}, P2={p2}, P3={p3})")

    return out
