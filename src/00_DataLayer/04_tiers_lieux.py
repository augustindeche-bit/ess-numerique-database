"""
Téléchargement des fichiers Tiers-Lieux (France Tiers-Lieux)

Source : https://www.data.gouv.fr/datasets/recensement-des-tiers-lieux-en-france-2023/
Fichiers :
    - bdftl-2023-01-fiche-identite.csv (~2,8 Mo) — identité, SIRET, adresse
    - bdftl-2023-04-activites.csv      (~2 Mo)   — activités, colonnes numériques
Destination : data/raw/

Usage : python src/00_DataLayer/download_tiers_lieux.py
"""

import urllib.request
import shutil
from pathlib import Path
from datetime import datetime

RAW_DIR = Path(__file__).resolve().parents[2] / "data" / "raw"

FILES = {
    "bdftl-2023-01-fiche-identite.csv": "https://static.data.gouv.fr/resources/recensement-des-tiers-lieux-en-france-2023/20241219-093636/bdftl-2023-01-fiche-identite.csv",
    "bdftl-2023-04-activites.csv": "https://static.data.gouv.fr/resources/recensement-des-tiers-lieux-en-france-2023/20241219-093639/bdftl-2023-04-activites.csv",
}


def download_file(name: str, url: str):
    dest = RAW_DIR / name
    if dest.exists():
        mod_time = datetime.fromtimestamp(dest.stat().st_mtime)
        size_mb = dest.stat().st_size / (1024 * 1024)
        print(f"  Fichier existant : {size_mb:.1f} Mo, modifié le {mod_time:%Y-%m-%d %H:%M}")
        answer = input(f"  Re-télécharger {name} ? [o/N] ").strip().lower()
        if answer != "o":
            print("  Ignoré.")
            return

    tmp = dest.with_suffix(".csv.tmp")
    print(f"  Téléchargement : {url}")
    with urllib.request.urlopen(url) as response:
        total = int(response.headers.get("Content-Length", 0))
        downloaded = 0
        with open(tmp, "wb") as f:
            while True:
                chunk = response.read(1024 * 1024)
                if not chunk:
                    break
                f.write(chunk)
                downloaded += len(chunk)
                if total:
                    print(f"\r  {downloaded / (1024**2):.1f} / {total / (1024**2):.1f} Mo ({downloaded/total*100:.0f}%)", end="", flush=True)
    shutil.move(str(tmp), str(dest))
    print(f"\n  OK : {dest.stat().st_size / (1024**2):.1f} Mo")


def main():
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    for name, url in FILES.items():
        print(f"\n--- {name} ---")
        download_file(name, url)


if __name__ == "__main__":
    main()
