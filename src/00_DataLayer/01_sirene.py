"""
Téléchargement du fichier StockUniteLegale (INSEE / Sirene)

Source : https://www.data.gouv.fr/fr/datasets/base-sirene-des-entreprises-et-de-leurs-etablissements-siren-siret
Fichier : StockUniteLegale_utf8.parquet (~680 Mo, ~30M lignes, actualisé mensuellement)
Destination : data/raw/StockUniteLegale_utf8.parquet

Usage : python notebooks/00_DataLayer/download_stock_unite_legale.py
"""

import urllib.request
import shutil
from pathlib import Path
from datetime import datetime

URL = "https://object.files.data.gouv.fr/data-pipeline-open/siren/stock/StockUniteLegale_utf8.parquet"
DEST = Path(__file__).resolve().parents[2] / "data" / "raw" / "StockUniteLegale_utf8.parquet"


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
    tmp_dest = DEST.with_suffix(".parquet.tmp")

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
    import pyarrow.parquet as pq

    pf = pq.ParquetFile(DEST)
    print(f"Row groups : {pf.metadata.num_row_groups}")
    print(f"Lignes     : {pf.metadata.num_rows:,}")
    print(f"Colonnes   : {pf.schema.names}")


if __name__ == "__main__":
    main()