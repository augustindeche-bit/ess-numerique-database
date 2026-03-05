"""Tests unitaires pour export_label_studio.py — export Label Studio."""

import importlib.util
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

# -- Import du module --

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

_spec = importlib.util.spec_from_file_location(
    "export_label_studio",
    ROOT / "src" / "03_Export" / "export_label_studio.py",
)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

filter_columns = _mod.filter_columns
generate_template = _mod.generate_template
main = _mod.main
COLUMNS_EXPORT = _mod.COLUMNS_EXPORT


# -- Helpers --


def _make_enriched(rows: list[dict]) -> pd.DataFrame:
    """Cree un DataFrame simulant x_enriched.csv."""
    return pd.DataFrame(rows)


def _enriched_row(**overrides) -> dict:
    """Ligne type enrichie avec colonnes metier + techniques."""
    base = {
        # Colonnes metier
        "siren": "111111111",
        "siret_siege": "11111111100012",
        "denomination": "ALPHA NUM SAS",
        "nom_usage": "DUPONT",
        "sigle": "AN",
        "naf": "62.01Z",
        "naf_numerique": True,
        "tags_numerique": "dev_logiciel",
        "categorie_juridique": "9220",
        "famille_ess": "association",
        "flag_ess_sirene": True,
        "est_active": True,
        "date_creation": "2015-03-10",
        "tranche_effectifs": "12",
        "annee_effectifs": "2022",
        "categorie_entreprise": "PME",
        "caractere_employeur": "O",
        "societe_mission": "O",
        "rna": "W751234567",
        "code_postal": "75001",
        "commune": "PARIS",
        "site_web": "https://alpha.example.com",
        "nom_structure_porteuse": np.nan,
        "sources": "sirene|ess_france",
        "origines": "ess_numerique",
        "methode_jointure": "siren_direct",
        "confiance_jointure": 1.0,
        # Colonnes techniques enrichissement
        "naf_enrich_source": "sirene",
        "naf_enrich_method": "siren_direct",
        "denomination_enrich_source": np.nan,
        "denomination_enrich_method": np.nan,
        "categorie_juridique_enrich_source": "sirene",
        "categorie_juridique_enrich_method": "siren_direct",
        "tranche_effectifs_enrich_source": "sirene",
        "tranche_effectifs_enrich_method": "siren_direct",
        "siret_siege_enrich_source": "sirene",
        "siret_siege_enrich_method": "siren_direct",
        "est_active_enrich_source": "sirene",
        "est_active_enrich_method": "siren_direct",
        "flag_ess_sirene_enrich_source": "sirene",
        "flag_ess_sirene_enrich_method": "siren_direct",
        "rna_enrich_source": np.nan,
        "rna_enrich_method": np.nan,
        # Colonnes de conflit
        "denomination__ess_numerique": "ALPHA NUM",
        "denomination__tiers_lieux": "Alpha Num SAS",
    }
    base.update(overrides)
    return base


# ================================================
# Tests : filter_columns
# ================================================


class TestFilterColumns:
    def test_keeps_business_columns(self):
        """Les colonnes metier sont conservees."""
        df = _make_enriched([_enriched_row()])
        result = filter_columns(df)
        for col in COLUMNS_EXPORT:
            assert col in result.columns, f"Colonne metier manquante : {col}"

    def test_removes_enrich_source(self):
        """Les colonnes _enrich_source sont supprimees."""
        df = _make_enriched([_enriched_row()])
        result = filter_columns(df)
        enrich_src = [c for c in result.columns if c.endswith("_enrich_source")]
        assert enrich_src == [], f"Colonnes non supprimees : {enrich_src}"

    def test_removes_enrich_method(self):
        """Les colonnes _enrich_method sont supprimees."""
        df = _make_enriched([_enriched_row()])
        result = filter_columns(df)
        enrich_meth = [c for c in result.columns if c.endswith("_enrich_method")]
        assert enrich_meth == [], f"Colonnes non supprimees : {enrich_meth}"

    def test_removes_conflict_columns(self):
        """Les colonnes __origine sont supprimees."""
        df = _make_enriched([_enriched_row()])
        result = filter_columns(df)
        conflict = [c for c in result.columns if "__" in c]
        assert conflict == [], f"Colonnes non supprimees : {conflict}"

    def test_preserves_values(self):
        """Les valeurs des colonnes metier sont intactes."""
        row = _enriched_row()
        df = _make_enriched([row])
        result = filter_columns(df)
        assert result.iloc[0]["siren"] == "111111111"
        assert result.iloc[0]["denomination"] == "ALPHA NUM SAS"
        assert result.iloc[0]["naf"] == "62.01Z"
        assert result.iloc[0]["famille_ess"] == "association"

    def test_missing_optional_column(self):
        """Une colonne metier absente du CSV source est simplement ignoree."""
        df = _make_enriched([{"siren": "111111111", "denomination": "TEST"}])
        result = filter_columns(df)
        assert "siren" in result.columns
        assert "denomination" in result.columns
        # site_web absent du source → absent du resultat (pas d'erreur)
        assert "site_web" not in result.columns

    def test_column_order(self):
        """Les colonnes sont dans l'ordre defini par COLUMNS_EXPORT."""
        df = _make_enriched([_enriched_row()])
        result = filter_columns(df)
        expected_order = [c for c in COLUMNS_EXPORT if c in result.columns]
        assert list(result.columns) == expected_order

    def test_multiple_rows(self):
        """Le filtrage fonctionne sur plusieurs lignes."""
        rows = [
            _enriched_row(siren="111111111"),
            _enriched_row(siren="222222222", denomination="BETA"),
        ]
        df = _make_enriched(rows)
        result = filter_columns(df)
        assert len(result) == 2
        assert result.iloc[0]["siren"] == "111111111"
        assert result.iloc[1]["siren"] == "222222222"


# ================================================
# Tests : generate_template
# ================================================


class TestGenerateTemplate:
    def test_valid_xml(self):
        """Le template est du XML valide."""
        template = generate_template()
        # xml.etree.ElementTree leve une exception si le XML est invalide
        root = ET.fromstring(template)
        assert root.tag == "View"

    def test_contains_classification_choices(self):
        """Le template contient les choix OUI / NON / A ENRICHIR."""
        template = generate_template()
        root = ET.fromstring(template)
        choices = root.findall(".//Choice")
        values = {c.get("value") for c in choices}
        assert "OUI" in values
        assert "NON" in values
        assert "A ENRICHIR" in values

    def test_contains_key_fields(self):
        """Le template affiche les champs cles."""
        template = generate_template()
        assert "$denomination" in template
        assert "$naf" in template
        assert "$famille_ess" in template
        assert "$site_web" in template
        assert "$siren" in template

    def test_contains_textarea_categorie(self):
        """Le template contient un champ texte pour la categorie."""
        template = generate_template()
        root = ET.fromstring(template)
        textareas = root.findall(".//TextArea")
        names = {ta.get("name") for ta in textareas}
        assert "categorie_activite" in names

    def test_contains_textarea_commentaire(self):
        """Le template contient un champ texte pour le commentaire."""
        template = generate_template()
        root = ET.fromstring(template)
        textareas = root.findall(".//TextArea")
        names = {ta.get("name") for ta in textareas}
        assert "commentaire" in names


# ================================================
# Tests : full export (bout en bout)
# ================================================


class TestFullExport:
    def test_end_to_end(self, tmp_path):
        """Test bout en bout avec donnees simulees."""
        # Creer un x_enriched.csv simule
        rows = [
            _enriched_row(siren="111111111"),
            _enriched_row(
                siren="222222222",
                denomination="BETA COOP",
                naf="62.02A",
                tags_numerique="conseil_it",
                famille_ess="cooperative",
            ),
        ]
        enriched_path = tmp_path / "x_enriched.csv"
        pd.DataFrame(rows).to_csv(enriched_path, index=False)

        csv_out = tmp_path / "x_label_studio.csv"
        xml_out = tmp_path / "label_studio_template.xml"

        # Executer le pipeline
        df = main(
            enriched_path=enriched_path,
            csv_output_path=csv_out,
            xml_output_path=xml_out,
        )

        # Verifier les fichiers de sortie
        assert csv_out.exists()
        assert xml_out.exists()

        # Verifier le CSV
        assert len(df) == 2
        assert "siren" in df.columns
        assert "denomination" in df.columns
        # Pas de colonnes techniques
        assert "naf_enrich_source" not in df.columns
        assert "denomination__ess_numerique" not in df.columns

        # Verifier le contenu
        assert df.iloc[0]["siren"] == "111111111"
        assert df.iloc[1]["denomination"] == "BETA COOP"

        # Verifier le XML est valide
        xml_content = xml_out.read_text(encoding="utf-8")
        root = ET.fromstring(xml_content)
        assert root.tag == "View"

    def test_csv_reloadable(self, tmp_path):
        """Le CSV exporte est rechargeable avec les bons types."""
        rows = [_enriched_row()]
        enriched_path = tmp_path / "x_enriched.csv"
        pd.DataFrame(rows).to_csv(enriched_path, index=False)

        csv_out = tmp_path / "x_label_studio.csv"
        xml_out = tmp_path / "label_studio_template.xml"

        main(
            enriched_path=enriched_path,
            csv_output_path=csv_out,
            xml_output_path=xml_out,
        )

        # Recharger le CSV
        df_reload = pd.read_csv(csv_out, dtype={"siren": str})
        assert df_reload.iloc[0]["siren"] == "111111111"
        assert len(df_reload.columns) == len(COLUMNS_EXPORT)

    def test_empty_enriched(self, tmp_path):
        """Un fichier enrichi vide produit un CSV vide (pas d'erreur)."""
        enriched_path = tmp_path / "x_enriched.csv"
        pd.DataFrame(columns=["siren", "denomination", "naf"]).to_csv(
            enriched_path, index=False
        )

        csv_out = tmp_path / "x_label_studio.csv"
        xml_out = tmp_path / "label_studio_template.xml"

        df = main(
            enriched_path=enriched_path,
            csv_output_path=csv_out,
            xml_output_path=xml_out,
        )

        assert len(df) == 0
        assert csv_out.exists()
        assert xml_out.exists()
