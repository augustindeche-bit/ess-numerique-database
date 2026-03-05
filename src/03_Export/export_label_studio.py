"""
Export pour Label Studio (US B.3)

Prepare un CSV simplifie et un template XML d'annotation
a partir du fichier enrichi x_enriched.csv.

Colonnes exportees : uniquement les colonnes metier (pas de tracabilite
_enrich_source/_enrich_method ni de colonnes de conflit __origine).

Entrees :
    - data/processed/candidates/x_enriched.csv

Sorties :
    - data/processed/export/x_label_studio.csv
    - data/processed/export/label_studio_template.xml

Usage : python src/03_Export/export_label_studio.py
"""

import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

ENRICHED_PATH = ROOT / "data" / "processed" / "candidates" / "x_enriched.csv"
OUTPUT_DIR = ROOT / "data" / "processed" / "export"
CSV_OUTPUT_PATH = OUTPUT_DIR / "x_label_studio.csv"
XML_OUTPUT_PATH = OUTPUT_DIR / "label_studio_template.xml"

# Colonnes metier a conserver dans l'export
COLUMNS_EXPORT = [
    "siren",
    "siret_siege",
    "denomination",
    "nom_usage",
    "sigle",
    "naf",
    "naf_numerique",
    "tags_numerique",
    "categorie_juridique",
    "famille_ess",
    "flag_ess_sirene",
    "est_active",
    "date_creation",
    "tranche_effectifs",
    "annee_effectifs",
    "categorie_entreprise",
    "caractere_employeur",
    "societe_mission",
    "rna",
    "code_postal",
    "commune",
    "site_web",
    "nom_structure_porteuse",
    "sources",
    "origines",
    "methode_jointure",
    "confiance_jointure",
    "score_enrichissement",
]

# Champs metier utilises pour calculer le score d'enrichissement (0-1).
# Plus le score est bas, plus la ligne a besoin d'etre enrichie.
SCORE_FIELDS = [
    "denomination",
    "naf",
    "categorie_juridique",
    "famille_ess",
    "siret_siege",
    "code_postal",
    "commune",
    "tranche_effectifs",
    "date_creation",
    "est_active",
    "site_web",
    "rna",
]


# --------------------------------------------------
# Filtrage des colonnes
# --------------------------------------------------


def compute_enrichment_score(df: pd.DataFrame) -> pd.Series:
    """Calcule un score d'enrichissement entre 0 et 1 pour chaque ligne.

    Score = nombre de champs metier remplis / nombre total de champs.
    Un score bas signifie que la ligne est quasi vide et a besoin d'enrichissement.
    """
    fields = [f for f in SCORE_FIELDS if f in df.columns]
    filled = df[fields].notna().sum(axis=1)
    return (filled / len(fields)).round(2)


def filter_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Supprime les colonnes techniques, garde uniquement les colonnes metier.

    Colonnes supprimees :
    - *_enrich_source  (tracabilite enrichissement)
    - *_enrich_method  (tracabilite enrichissement)
    - *__*             (colonnes de conflit entre origines)
    """
    cols_to_keep = [c for c in COLUMNS_EXPORT if c in df.columns]
    return df[cols_to_keep].copy()


# --------------------------------------------------
# Template XML Label Studio
# --------------------------------------------------


LABEL_STUDIO_TEMPLATE = """\
<View>
  <Header value="Structure ESS - Validation"/>

  <View style="box-shadow: 2px 2px 5px #999; padding: 20px; margin-top: 1em; border-radius: 5px;">
    <Header value="Identite"/>
    <View style="display: grid; grid-template-columns: 1fr 1fr; column-gap: 1em;">
      <Text name="siren" value="SIREN : $siren"/>
      <Text name="siret" value="SIRET siege : $siret_siege"/>
      <Text name="denomination" value="Denomination : $denomination"/>
      <Text name="nom_usage" value="Nom d'usage : $nom_usage"/>
      <Text name="sigle" value="Sigle : $sigle"/>
      <Text name="rna" value="RNA : $rna"/>
    </View>
  </View>

  <View style="box-shadow: 2px 2px 5px #999; padding: 20px; margin-top: 1em; border-radius: 5px;">
    <Header value="Classification"/>
    <View style="display: grid; grid-template-columns: 1fr 1fr; column-gap: 1em;">
      <Text name="naf" value="NAF : $naf"/>
      <Text name="naf_numerique" value="NAF numerique : $naf_numerique"/>
      <Text name="tags_numerique" value="Tags numerique : $tags_numerique"/>
      <Text name="categorie_juridique" value="Cat. juridique : $categorie_juridique"/>
      <Text name="famille_ess" value="Famille ESS : $famille_ess"/>
      <Text name="flag_ess_sirene" value="Flag ESS SIRENE : $flag_ess_sirene"/>
    </View>
  </View>

  <View style="box-shadow: 2px 2px 5px #999; padding: 20px; margin-top: 1em; border-radius: 5px;">
    <Header value="Activite"/>
    <View style="display: grid; grid-template-columns: 1fr 1fr; column-gap: 1em;">
      <Text name="est_active" value="Active : $est_active"/>
      <Text name="date_creation" value="Date creation : $date_creation"/>
      <Text name="tranche_effectifs" value="Effectifs : $tranche_effectifs"/>
      <Text name="categorie_entreprise" value="Categorie : $categorie_entreprise"/>
      <Text name="caractere_employeur" value="Employeur : $caractere_employeur"/>
      <Text name="societe_mission" value="Societe a mission : $societe_mission"/>
    </View>
  </View>

  <View style="box-shadow: 2px 2px 5px #999; padding: 20px; margin-top: 1em; border-radius: 5px;">
    <Header value="Localisation et web"/>
    <View style="display: grid; grid-template-columns: 1fr 1fr; column-gap: 1em;">
      <Text name="code_postal" value="Code postal : $code_postal"/>
      <Text name="commune" value="Commune : $commune"/>
      <Text name="site_web" value="Site web : $site_web"/>
      <Text name="nom_structure_porteuse" value="Structure porteuse : $nom_structure_porteuse"/>
    </View>
  </View>

  <View style="box-shadow: 2px 2px 5px #999; padding: 20px; margin-top: 1em; border-radius: 5px;">
    <Header value="Provenance"/>
    <View style="display: grid; grid-template-columns: 1fr 1fr; column-gap: 1em;">
      <Text name="sources" value="Sources : $sources"/>
      <Text name="origines" value="Origines : $origines"/>
      <Text name="methode_jointure" value="Methode jointure : $methode_jointure"/>
      <Text name="confiance_jointure" value="Confiance : $confiance_jointure"/>
      <Text name="score_enrichissement" value="Score enrichissement : $score_enrichissement"/>
    </View>
  </View>

  <Header value="Classification ESS numerique" style="margin-top: 2em;"/>

  <Choices name="classification" toName="denomination" choice="single-radio" showInline="true">
    <Choice value="OUI" alias="oui"/>
    <Choice value="NON" alias="non"/>
    <Choice value="A ENRICHIR" alias="a_enrichir"/>
  </Choices>

  <Header value="Categorie d'activite numerique (si OUI)" style="margin-top: 1em;"/>
  <TextArea name="categorie_activite" toName="denomination"
            placeholder="Ex: mediation numerique, dev logiciel, FAI..."
            rows="2" maxSubmissions="1"/>

  <Header value="Commentaire / source de la decision" style="margin-top: 1em;"/>
  <TextArea name="commentaire" toName="denomination"
            placeholder="Justification, lien vers le site web..."
            rows="3" maxSubmissions="1"/>
</View>
"""


def generate_template() -> str:
    """Retourne le template XML Label Studio."""
    return LABEL_STUDIO_TEMPLATE


# --------------------------------------------------
# Main
# --------------------------------------------------


def main(
    enriched_path: Path = ENRICHED_PATH,
    csv_output_path: Path = CSV_OUTPUT_PATH,
    xml_output_path: Path = XML_OUTPUT_PATH,
):
    print("=" * 60)
    print("Export pour Label Studio (US B.3)")
    print("=" * 60)

    # --- 1. Charger x_enriched.csv ---
    print("\n1. Chargement du fichier enrichi...")
    df = pd.read_csv(enriched_path, dtype={"siren": str})
    print(f"   {len(df):,} lignes, {len(df.columns)} colonnes")

    # --- 2. Score d'enrichissement ---
    print("\n2. Calcul du score d'enrichissement...")
    df["score_enrichissement"] = compute_enrichment_score(df)
    n_low = (df["score_enrichissement"] < 0.5).sum()
    n_high = (df["score_enrichissement"] >= 0.8).sum()
    print(f"   Score moyen : {df['score_enrichissement'].mean():.2f}")
    print(f"   Lignes quasi vides (score < 0.5) : {n_low:,}")
    print(f"   Lignes bien remplies (score >= 0.8) : {n_high:,}")

    # --- 3. Filtrer les colonnes ---
    print("\n3. Filtrage des colonnes techniques...")
    df_export = filter_columns(df)
    n_removed = len(df.columns) - len(df_export.columns)
    print(f"   {len(df_export.columns)} colonnes conservees ({n_removed} supprimees)")

    # --- 4. Export CSV ---
    csv_output_path.parent.mkdir(parents=True, exist_ok=True)
    df_export.to_csv(csv_output_path, index=False, encoding="utf-8-sig")
    print(f"\n4. Export CSV : {csv_output_path} ({len(df_export):,} lignes)")

    # --- 5. Export template XML ---
    template = generate_template()
    xml_output_path.write_text(template, encoding="utf-8")
    print(f"5. Export XML : {xml_output_path}")

    return df_export


if __name__ == "__main__":
    main()
