"""
Pipeline complet ESS numerique.

Enchaine les etapes du pipeline dans l'ordre :
  0. Telechargement des sources brutes  (--download, desactive par defaut)
  1. Filtrage des candidats              (4 scripts paralleles)
  2. Consolidation par SIREN
  3. Enrichissement depuis SIRENE
  4. Export pour Label Studio

Les fichiers de sortie sont nettoyes avant chaque etape pour eviter
de travailler sur des donnees obsoletes.

Usage :
    python run_pipeline.py               # etapes 1-4 (sans telechargement)
    python run_pipeline.py --download     # etapes 0-4 (avec telechargement)
    python run_pipeline.py --from 2       # reprend a partir de l'etape 2
    python run_pipeline.py --step 1 3     # joue uniquement les etapes 1 et 3
    python run_pipeline.py --step 2       # joue uniquement l'etape 2
"""

import argparse
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
PYTHON = sys.executable

# ──────────────────────────────────────────────
# Fichiers de sortie par etape (a nettoyer)
# ──────────────────────────────────────────────

RAW_FILES = [
    ROOT / "data" / "raw" / "StockUniteLegale_utf8.parquet",
    ROOT / "data" / "raw" / "entreprisesess.csv",
    ROOT / "data" / "raw" / "liste_nationale_esus.xlsx",
    ROOT / "data" / "raw" / "bdftl-2023-01-fiche-identite.csv",
    ROOT / "data" / "raw" / "bdftl-2023-04-activites.csv",
]

CANDIDATES_FILES = [
    ROOT / "data" / "processed" / "candidates" / "01_ess_x_naf.csv",
    ROOT / "data" / "processed" / "candidates" / "02_cate_x_naf.csv",
    ROOT / "data" / "processed" / "candidates" / "03_tiers_lieux.csv",
    ROOT / "data" / "processed" / "candidates" / "04_ess_flag_insee.csv",
]

CONSOLIDATED_FILE = ROOT / "data" / "processed" / "candidates" / "x_consolidated.csv"
ENRICHED_FILE = ROOT / "data" / "processed" / "candidates" / "x_enriched.csv"

EXPORT_FILES = [
    ROOT / "data" / "processed" / "export" / "x_label_studio.csv",
    ROOT / "data" / "processed" / "export" / "label_studio_template.xml",
]

# ──────────────────────────────────────────────
# Scripts par etape
# ──────────────────────────────────────────────

STEP_0_SCRIPTS = [
    ROOT / "src" / "00_DataLayer" / "01_sirene.py",
    ROOT / "src" / "00_DataLayer" / "02_ess_france.py",
    ROOT / "src" / "00_DataLayer" / "03_esus.py",
    ROOT / "src" / "00_DataLayer" / "04_tiers_lieux.py",
]

STEP_1_SCRIPTS = [
    ROOT / "src" / "01_Candidates" / "01_ess_x_naf.py",
    ROOT / "src" / "01_Candidates" / "02_cate_x_naf.py",
    ROOT / "src" / "01_Candidates" / "03_tiers_lieux.py",
    ROOT / "src" / "01_Candidates" / "04_ess_flag_insee.py",
]

STEP_2_SCRIPT = ROOT / "src" / "01_Candidates" / "x_consolated.py"
STEP_3_SCRIPT = ROOT / "src" / "02_DataEnrichment" / "enrich_from_sirene.py"
STEP_4_SCRIPT = ROOT / "src" / "03_Export" / "export_label_studio.py"


# ──────────────────────────────────────────────
# Utilitaires
# ──────────────────────────────────────────────


def clean_files(files: list[Path]) -> int:
    """Supprime les fichiers existants. Retourne le nombre de fichiers supprimes."""
    removed = 0
    for f in files:
        if f.exists():
            f.unlink()
            print(f"   supprime : {f.relative_to(ROOT)}")
            removed += 1
    return removed


def run_script(script: Path) -> None:
    """Execute un script Python et propage les erreurs."""
    print(f"\n   -> {script.relative_to(ROOT)}")
    result = subprocess.run(
        [PYTHON, str(script)],
        cwd=str(ROOT),
    )
    if result.returncode != 0:
        print(f"\n   ERREUR : {script.name} a echoue (code {result.returncode})")
        sys.exit(result.returncode)


def format_duration(seconds: float) -> str:
    """Formate une duree en min:sec ou sec."""
    if seconds >= 60:
        m, s = divmod(int(seconds), 60)
        return f"{m}min {s}s"
    return f"{seconds:.1f}s"


# ──────────────────────────────────────────────
# Etapes du pipeline
# ──────────────────────────────────────────────


def step_0_download():
    """Etape 0 : Telechargement des sources brutes."""
    print("\n" + "=" * 60)
    print("ETAPE 0 — Telechargement des sources brutes")
    print("=" * 60)

    n = clean_files(RAW_FILES)
    if n:
        print(f"   {n} fichier(s) nettoye(s)")

    for script in STEP_0_SCRIPTS:
        run_script(script)


def step_1_candidates():
    """Etape 1 : Filtrage des candidats."""
    print("\n" + "=" * 60)
    print("ETAPE 1 — Filtrage des candidats")
    print("=" * 60)

    n = clean_files(CANDIDATES_FILES)
    if n:
        print(f"   {n} fichier(s) nettoye(s)")

    for script in STEP_1_SCRIPTS:
        run_script(script)


def step_2_consolidation():
    """Etape 2 : Consolidation par SIREN."""
    print("\n" + "=" * 60)
    print("ETAPE 2 — Consolidation par SIREN")
    print("=" * 60)

    n = clean_files([CONSOLIDATED_FILE])
    if n:
        print(f"   {n} fichier(s) nettoye(s)")

    run_script(STEP_2_SCRIPT)


def step_3_enrichment():
    """Etape 3 : Enrichissement depuis SIRENE."""
    print("\n" + "=" * 60)
    print("ETAPE 3 — Enrichissement depuis SIRENE")
    print("=" * 60)

    n = clean_files([ENRICHED_FILE])
    if n:
        print(f"   {n} fichier(s) nettoye(s)")

    run_script(STEP_3_SCRIPT)


def step_4_export():
    """Etape 4 : Export pour Label Studio."""
    print("\n" + "=" * 60)
    print("ETAPE 4 — Export pour Label Studio")
    print("=" * 60)

    n = clean_files(EXPORT_FILES)
    if n:
        print(f"   {n} fichier(s) nettoye(s)")

    run_script(STEP_4_SCRIPT)


# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────

STEPS = {
    0: ("Telechargement", step_0_download),
    1: ("Candidats", step_1_candidates),
    2: ("Consolidation", step_2_consolidation),
    3: ("Enrichissement", step_3_enrichment),
    4: ("Export", step_4_export),
}


def main():
    parser = argparse.ArgumentParser(
        description="Pipeline complet ESS numerique.",
    )
    parser.add_argument(
        "--download",
        action="store_true",
        help="Inclure l'etape 0 (telechargement des sources brutes).",
    )
    parser.add_argument(
        "--from",
        dest="from_step",
        type=int,
        choices=range(5),
        default=None,
        help="Reprendre a partir de l'etape N (0-4).",
    )
    parser.add_argument(
        "--step",
        nargs="+",
        type=int,
        choices=range(5),
        default=None,
        help="Jouer uniquement les etapes indiquees (ex: --step 1 3).",
    )
    args = parser.parse_args()

    # Determiner les etapes a executer
    if args.step is not None:
        selected = sorted(set(args.step))
        steps_to_run = [(n, name, fn) for n, (name, fn) in STEPS.items() if n in selected]
    elif args.from_step is not None:
        start = args.from_step
        steps_to_run = [(n, name, fn) for n, (name, fn) in STEPS.items() if n >= start]
    elif args.download:
        steps_to_run = [(n, name, fn) for n, (name, fn) in STEPS.items()]
    else:
        steps_to_run = [(n, name, fn) for n, (name, fn) in STEPS.items() if n >= 1]

    print("=" * 60)
    print("PIPELINE ESS NUMERIQUE")
    print("=" * 60)
    print(f"\nEtapes a executer : {', '.join(f'{n}-{name}' for n, name, _ in steps_to_run)}")

    t_start = time.time()

    for step_n, step_name, step_fn in steps_to_run:
        t_step = time.time()
        step_fn()
        dt = time.time() - t_step
        print(f"\n   Etape {step_n} terminee en {format_duration(dt)}")

    dt_total = time.time() - t_start
    print("\n" + "=" * 60)
    print(f"PIPELINE TERMINE en {format_duration(dt_total)}")
    print("=" * 60)


if __name__ == "__main__":
    main()
