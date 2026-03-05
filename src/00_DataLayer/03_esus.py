"""
Téléchargement de la liste nationale des agréments ESUS

Source : https://www.tresor.economie.gouv.fr/banque-assurance-finance/finance-sociale-et-solidaire/liste-nationale-agrements-esus
Fichier : Liste nationale ESUS (format xlsx, ~300 Ko)
Destination : data/raw/liste_nationale_esus.xlsx

Usage : python src/00_DataLayer/download_esus.py
"""

import urllib.request
import shutil
from pathlib import Path
from datetime import datetime

URL = "https://www.tresor.economie.gouv.fr/Institutionnel/Niveau3/Pages/fbd397c7-b0a9-42a3-8b07-5d50637b634e/files/1b886032-0134-4b10-8c52-e895b8e8922e"
DEST = Path(__file__).resolve().parents[2] / "data" / "raw" / "liste_nationale_esus.xlsx"


def main():
    DEST.parent.mkdir(parents=True, exist_ok=True)

    # --- Vérification du fichier existant ---
    if DEST.exists():
        mod_time = datetime.fromtimestamp(DEST.stat().st_mtime)
        size_kb = DEST.stat().st_size / 1024
        print(f"Fichier existant : {size_kb:.0f} Ko, modifié le {mod_time:%Y-%m-%d %H:%M}")
        answer = input("Re-télécharger ? [o/N] ").strip().lower()
        if answer != "o":
            print("Téléchargement annulé.")
            return

    # --- Téléchargement ---
    tmp_dest = DEST.with_suffix(".xlsx.tmp")

    print(f"Téléchargement depuis tresor.economie.gouv.fr...")
    print(f"URL  : {URL}")
    print(f"Dest : {DEST}")
    print()

    req = urllib.request.Request(URL, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req) as response:
        with open(tmp_dest, "wb") as f:
            shutil.copyfileobj(response, f)

    shutil.move(str(tmp_dest), str(DEST))
    print(f"Téléchargement terminé : {DEST.stat().st_size / 1024:.0f} Ko")

    # --- Vérification rapide ---
    import pandas as pd

    df = pd.read_excel(DEST)
    print(f"Lignes   : {len(df):,}")
    print(f"Colonnes : {list(df.columns)}")


if __name__ == "__main__":
    main()
