"""
Téléchargement de la liste des entreprises de l'ESS

Source : https://www.data.gouv.fr/fr/datasets/liste-des-entreprises-de-less
Fichier : entreprisesess.csv (~129 Mo, ~1,3 million de lignes)
Destination : data/raw/entreprisesess.csv

Usage : python notebooks/00_DataLayer/download_entreprises_ess.py
"""

import urllib.request
import shutil
from pathlib import Path
from datetime import datetime

URL = "https://static.data.gouv.fr/resources/liste-des-entreprises-de-less/20231120-102604/entreprisesess.csv"
DEST = Path(__file__).resolve().parents[2] / "data" / "raw" / "entreprisesess.csv"


def main():
    DEST.parent.mkdir(parents=True, exist_ok=True)

    # --- Vérification du fichier existant ---
    if DEST.exists():
        mod_time = datetime.fromtimestamp(DEST.stat().st_mtime)
        size_mb = DEST.stat().st_size / (1024 * 1024)
        print(f"Fichier existant : {size_mb:.0f} Mo, modifié le {mod_time:%Y-%m-%d %H:%M}")
        answer = input("Re-télécharger ? [o/N] ").strip().lower()
        if answer != "o":
            print("Téléchargement annulé.")
            return

    # --- Téléchargement ---
    tmp_dest = DEST.with_suffix(".csv.tmp")

    print(f"Téléchargement depuis data.gouv.fr...")
    print(f"URL  : {URL}")
    print(f"Dest : {DEST}")
    print()

    with urllib.request.urlopen(URL) as response:
        total = int(response.headers.get("Content-Length", 0))
        downloaded = 0
        chunk_size = 1024 * 1024  # 1 Mo

        with open(tmp_dest, "wb") as f:
            while True:
                chunk = response.read(chunk_size)
                if not chunk:
                    break
                f.write(chunk)
                downloaded += len(chunk)
                if total:
                    pct = downloaded / total * 100
                    print(f"\r  {downloaded / (1024**2):.0f} / {total / (1024**2):.0f} Mo ({pct:.0f}%)", end="", flush=True)

    shutil.move(str(tmp_dest), str(DEST))
    print(f"\n\nTéléchargement terminé : {DEST.stat().st_size / (1024**2):.0f} Mo")

    # --- Vérification rapide ---
    import pandas as pd

    df = pd.read_csv(DEST, nrows=5)
    total_lines = sum(1 for _ in open(DEST, encoding="utf-8")) - 1
    print(f"Lignes   : ~{total_lines:,}")
    print(f"Colonnes : {list(df.columns)}")


if __name__ == "__main__":
    main()
