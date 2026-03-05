"""Tests unitaires pour enrich_from_sirene.py — enrichissement depuis SIRENE."""

import importlib.util
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

# ── Import du module ──

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

_spec = importlib.util.spec_from_file_location(
    "enrich_from_sirene",
    ROOT / "src" / "02_DataEnrichment" / "enrich_from_sirene.py",
)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

load_sirene_for_sirens = _mod.load_sirene_for_sirens
enrich_field = _mod.enrich_field
compute_siret_siege = _mod.compute_siret_siege
re_derive_from_enriched = _mod.re_derive_from_enriched
main = _mod.main
SIRENE_COLS = _mod.SIRENE_COLS
SIRENE_FIELD_MAPPING = _mod.SIRENE_FIELD_MAPPING


# ── Helpers ──


def _make_parquet(path: Path, rows: list[dict]) -> Path:
    """Écrit un parquet simulé avec les colonnes SIRENE attendues."""
    df = pd.DataFrame(rows)
    # S'assurer que toutes les colonnes attendues existent
    for col in SIRENE_COLS:
        if col not in df.columns:
            df[col] = None
    table = pa.Table.from_pandas(df)
    pq.write_table(table, path)
    return path


def _make_consolidated(rows: list[dict]) -> pd.DataFrame:
    """Crée un DataFrame simulant x_consolidated.csv."""
    return pd.DataFrame(rows)


# ── Données simulées SIRENE ──

SIRENE_ROWS = [
    {
        "siren": "111111111",
        "dateDebut": "2023-01-01",
        "nicSiegeUniteLegale": "00012",
        "activitePrincipaleUniteLegale": "62.01Z",
        "categorieJuridiqueUniteLegale": "9220",
        "trancheEffectifsUniteLegale": "12",
        "denominationUniteLegale": "ALPHA NUM SAS",
        "nomUniteLegale": "DUPONT",
        "sigleUniteLegale": "AN",
        "dateCreationUniteLegale": "2015-03-10",
        "etatAdministratifUniteLegale": "A",
        "identifiantAssociationUniteLegale": "W751234567",
        "economieSocialeSolidaireUniteLegale": "O",
        "caractereEmployeurUniteLegale": "O",
        "categorieEntreprise": "PME",
        "societeMissionUniteLegale": "O",
        "anneeEffectifsUniteLegale": "2022",
    },
    {
        "siren": "222222222",
        "dateDebut": "2024-06-01",
        "nicSiegeUniteLegale": "00025",
        "activitePrincipaleUniteLegale": "62.02A",
        "categorieJuridiqueUniteLegale": "5458",
        "trancheEffectifsUniteLegale": "22",
        "denominationUniteLegale": "BETA COOP",
        "nomUniteLegale": None,
        "sigleUniteLegale": "BC",
        "dateCreationUniteLegale": "2018-07-20",
        "etatAdministratifUniteLegale": "A",
        "identifiantAssociationUniteLegale": None,
        "economieSocialeSolidaireUniteLegale": "O",
        "caractereEmployeurUniteLegale": "N",
        "categorieEntreprise": "ETI",
        "societeMissionUniteLegale": None,
        "anneeEffectifsUniteLegale": "2023",
    },
    # Ligne plus ancienne pour 222222222 — doit être dédupliquée
    {
        "siren": "222222222",
        "dateDebut": "2020-01-01",
        "nicSiegeUniteLegale": "00099",
        "activitePrincipaleUniteLegale": "01.11Z",
        "categorieJuridiqueUniteLegale": "5458",
        "trancheEffectifsUniteLegale": "11",
        "denominationUniteLegale": "BETA COOP ANCIEN",
        "nomUniteLegale": None,
        "sigleUniteLegale": "BCA",
        "dateCreationUniteLegale": "2018-07-20",
        "etatAdministratifUniteLegale": "A",
        "identifiantAssociationUniteLegale": None,
        "economieSocialeSolidaireUniteLegale": None,
        "caractereEmployeurUniteLegale": "N",
        "categorieEntreprise": "PME",
        "societeMissionUniteLegale": None,
        "anneeEffectifsUniteLegale": "2020",
    },
    {
        "siren": "333333333",
        "dateDebut": "2022-09-15",
        "nicSiegeUniteLegale": "00038",
        "activitePrincipaleUniteLegale": "62.03Z",
        "categorieJuridiqueUniteLegale": "9210",
        "trancheEffectifsUniteLegale": "NN",
        "denominationUniteLegale": "GAMMA LAB",
        "nomUniteLegale": "MARTIN",
        "sigleUniteLegale": None,
        "dateCreationUniteLegale": "2020-01-05",
        "etatAdministratifUniteLegale": "C",
        "identifiantAssociationUniteLegale": None,
        "economieSocialeSolidaireUniteLegale": "N",
        "caractereEmployeurUniteLegale": "N",
        "categorieEntreprise": "GE",
        "societeMissionUniteLegale": None,
        "anneeEffectifsUniteLegale": "2021",
    },
]


@pytest.fixture
def parquet_path(tmp_path):
    """Crée un fichier parquet simulé dans un répertoire temporaire."""
    return _make_parquet(tmp_path / "StockUniteLegale_utf8.parquet", SIRENE_ROWS)


# ══════════════════════════════════════════════
# Tests : load_sirene_for_sirens
# ══════════════════════════════════════════════


class TestLoadSireneForSirens:
    def test_filter_by_sirens(self, parquet_path):
        result = load_sirene_for_sirens({"111111111"}, parquet_path)
        assert len(result) == 1
        assert "111111111" in result.index

    def test_filter_multiple(self, parquet_path):
        result = load_sirene_for_sirens(
            {"111111111", "333333333"}, parquet_path
        )
        assert len(result) == 2
        assert set(result.index) == {"111111111", "333333333"}

    def test_dedup_by_date(self, parquet_path):
        """222222222 a deux périodes ; seule la plus récente (2024) est gardée."""
        result = load_sirene_for_sirens({"222222222"}, parquet_path)
        assert len(result) == 1
        # La valeur la plus récente (2024-06-01) a denomination "BETA COOP"
        assert result.loc["222222222", "denominationUniteLegale"] == "BETA COOP"

    def test_columns_present(self, parquet_path):
        result = load_sirene_for_sirens({"111111111"}, parquet_path)
        for col, _, _ in SIRENE_FIELD_MAPPING:
            assert col in result.columns, f"Colonne manquante : {col}"
        assert "nicSiegeUniteLegale" in result.columns

    def test_unknown_siren_empty(self, parquet_path):
        result = load_sirene_for_sirens({"999999999"}, parquet_path)
        assert len(result) == 0

    def test_empty_set(self, parquet_path):
        result = load_sirene_for_sirens(set(), parquet_path)
        assert len(result) == 0


# ══════════════════════════════════════════════
# Tests : enrich_field
# ══════════════════════════════════════════════


class TestEnrichField:
    def _make_lookup(self):
        """Crée un lookup simulant le résultat de load_sirene_for_sirens."""
        return pd.DataFrame(
            {
                "activitePrincipaleUniteLegale": ["62.01Z", "62.02A"],
                "denominationUniteLegale": ["ALPHA NUM SAS", "BETA COOP"],
                "etatAdministratifUniteLegale": ["A", "C"],
                "economieSocialeSolidaireUniteLegale": ["O", "N"],
                "trancheEffectifsUniteLegale": ["12", "22"],
            },
            index=pd.Index(["111111111", "222222222"], name="siren"),
        )

    def test_fill_empty(self):
        """Champ vide → rempli depuis SIRENE."""
        df = _make_consolidated([
            {"siren": "111111111", "naf": np.nan},
        ])
        lookup = self._make_lookup()
        df = enrich_field(df, lookup, "activitePrincipaleUniteLegale", "naf")
        assert df.iloc[0]["naf"] == "62.01Z"
        assert df.iloc[0]["naf_enrich_source"] == "sirene"
        assert df.iloc[0]["naf_enrich_method"] == "siren_direct"

    def test_skip_if_filled(self):
        """Champ déjà rempli → pas écrasé."""
        df = _make_consolidated([
            {"siren": "111111111", "naf": "58.29A"},
        ])
        lookup = self._make_lookup()
        df = enrich_field(df, lookup, "activitePrincipaleUniteLegale", "naf")
        assert df.iloc[0]["naf"] == "58.29A"
        assert pd.isna(df.iloc[0]["naf_enrich_source"])

    def test_fill_empty_string(self):
        """Champ '' (chaîne vide) → considéré comme vide et rempli."""
        df = _make_consolidated([
            {"siren": "222222222", "denomination": ""},
        ])
        lookup = self._make_lookup()
        df = enrich_field(
            df, lookup, "denominationUniteLegale", "denomination"
        )
        assert df.iloc[0]["denomination"] == "BETA COOP"
        assert df.iloc[0]["denomination_enrich_source"] == "sirene"

    def test_transform_est_active(self):
        """Transform est_active : A → True."""
        df = _make_consolidated([
            {"siren": "111111111"},
        ])
        lookup = self._make_lookup()
        df = enrich_field(
            df, lookup, "etatAdministratifUniteLegale", "est_active", "est_active"
        )
        assert df.iloc[0]["est_active"] == True
        assert df.iloc[0]["est_active_enrich_source"] == "sirene"

    def test_transform_est_active_cessation(self):
        """Transform est_active : C → False."""
        df = _make_consolidated([
            {"siren": "222222222"},
        ])
        lookup = self._make_lookup()
        df = enrich_field(
            df, lookup, "etatAdministratifUniteLegale", "est_active", "est_active"
        )
        assert df.iloc[0]["est_active"] == False

    def test_transform_flag_ess(self):
        """Transform flag_ess : O → True."""
        df = _make_consolidated([
            {"siren": "111111111"},
        ])
        lookup = self._make_lookup()
        df = enrich_field(
            df, lookup, "economieSocialeSolidaireUniteLegale", "flag_ess_sirene", "flag_ess"
        )
        assert df.iloc[0]["flag_ess_sirene"] == True

    def test_no_sirene_match(self):
        """SIREN absent du lookup → pas d'enrichissement."""
        df = _make_consolidated([
            {"siren": "999999999", "naf": np.nan},
        ])
        lookup = self._make_lookup()
        df = enrich_field(df, lookup, "activitePrincipaleUniteLegale", "naf")
        assert pd.isna(df.iloc[0]["naf"])
        assert pd.isna(df.iloc[0]["naf_enrich_source"])

    def test_new_column_created(self):
        """Si la colonne cible n'existe pas, elle est créée."""
        df = _make_consolidated([
            {"siren": "111111111"},
        ])
        lookup = self._make_lookup()
        df = enrich_field(
            df, lookup, "trancheEffectifsUniteLegale", "tranche_effectifs"
        )
        assert "tranche_effectifs" in df.columns
        assert df.iloc[0]["tranche_effectifs"] == "12"


# ══════════════════════════════════════════════
# Tests : compute_siret_siege
# ══════════════════════════════════════════════


class TestComputeSiretSiege:
    def _make_lookup(self):
        return pd.DataFrame(
            {"nicSiegeUniteLegale": ["00012", "00025", "ABC"]},
            index=pd.Index(["111111111", "222222222", "333333333"], name="siren"),
        )

    def test_valid_siret(self):
        df = _make_consolidated([
            {"siren": "111111111"},
        ])
        lookup = self._make_lookup()
        df = compute_siret_siege(df, lookup)
        assert df.iloc[0]["siret_siege"] == "11111111100012"
        assert df.iloc[0]["siret_siege_enrich_source"] == "sirene"
        assert df.iloc[0]["siret_siege_enrich_method"] == "siren_direct"

    def test_skip_if_filled(self):
        df = _make_consolidated([
            {"siren": "111111111", "siret_siege": "11111111199999"},
        ])
        lookup = self._make_lookup()
        df = compute_siret_siege(df, lookup)
        assert df.iloc[0]["siret_siege"] == "11111111199999"
        assert pd.isna(df.iloc[0]["siret_siege_enrich_source"])

    def test_invalid_nic_ignored(self):
        """NIC non numérique ('ABC') → pas d'enrichissement."""
        df = _make_consolidated([
            {"siren": "333333333"},
        ])
        lookup = self._make_lookup()
        df = compute_siret_siege(df, lookup)
        assert pd.isna(df.iloc[0]["siret_siege"])

    def test_multiple_sirens(self):
        df = _make_consolidated([
            {"siren": "111111111"},
            {"siren": "222222222"},
        ])
        lookup = self._make_lookup()
        df = compute_siret_siege(df, lookup)
        assert df.iloc[0]["siret_siege"] == "11111111100012"
        assert df.iloc[1]["siret_siege"] == "22222222200025"


# ══════════════════════════════════════════════
# Tests : re_derive_from_enriched
# ══════════════════════════════════════════════


class TestReDeriveFromEnriched:
    def test_famille_ess_from_enriched_cj(self):
        """categorie_juridique enrichie → famille_ess re-dérivée."""
        df = _make_consolidated([
            {
                "siren": "111111111",
                "categorie_juridique": "9220",
                "categorie_juridique_enrich_source": "sirene",
                "famille_ess": np.nan,
                "naf": "01.11Z",
                "naf_numerique": False,
                "tags_numerique": np.nan,
            },
        ])
        df = re_derive_from_enriched(df)
        assert df.iloc[0]["famille_ess"] == "association"

    def test_naf_numerique_from_enriched_naf(self):
        """naf enrichi → naf_numerique et tags_numerique re-dérivés."""
        df = _make_consolidated([
            {
                "siren": "111111111",
                "naf": "62.01Z",
                "naf_enrich_source": "sirene",
                "naf_numerique": False,
                "tags_numerique": np.nan,
                "categorie_juridique": np.nan,
            },
        ])
        df = re_derive_from_enriched(df)
        assert df.iloc[0]["naf_numerique"] == True
        assert df.iloc[0]["tags_numerique"] == "dev_logiciel"

    def test_no_rederive_without_enrichment(self):
        """Pas de source enrichissement → pas de re-dérivation."""
        df = _make_consolidated([
            {
                "siren": "111111111",
                "categorie_juridique": "9220",
                "famille_ess": np.nan,
                "naf": "62.01Z",
                "naf_numerique": False,
                "tags_numerique": np.nan,
            },
        ])
        df = re_derive_from_enriched(df)
        # famille_ess reste NaN car pas de marqueur d'enrichissement
        assert pd.isna(df.iloc[0]["famille_ess"])
        # naf_numerique reste False
        assert df.iloc[0]["naf_numerique"] == False

    def test_cooperative_famille(self):
        """categorie_juridique coopérative enrichie → famille cooperative."""
        df = _make_consolidated([
            {
                "siren": "222222222",
                "categorie_juridique": "5458",
                "categorie_juridique_enrich_source": "sirene",
                "famille_ess": np.nan,
                "naf": "01.11Z",
                "naf_numerique": False,
                "tags_numerique": np.nan,
            },
        ])
        df = re_derive_from_enriched(df)
        assert df.iloc[0]["famille_ess"] == "cooperative"

    def test_naf_non_numerique(self):
        """NAF enrichi mais non numérique → naf_numerique reste False."""
        df = _make_consolidated([
            {
                "siren": "111111111",
                "naf": "01.11Z",
                "naf_enrich_source": "sirene",
                "naf_numerique": True,
                "tags_numerique": "dev_logiciel",
                "categorie_juridique": np.nan,
            },
        ])
        df = re_derive_from_enriched(df)
        assert df.iloc[0]["naf_numerique"] == False
        assert pd.isna(df.iloc[0]["tags_numerique"])


# ══════════════════════════════════════════════
# Test d'intégration : pipeline complet
# ══════════════════════════════════════════════


class TestFullPipeline:
    def test_full_pipeline(self, tmp_path, parquet_path):
        """Test bout en bout avec données simulées."""

        # --- Créer un x_consolidated.csv simulé ---
        consolidated_rows = [
            {
                "siren": "111111111",
                "sources": "sirene",
                "origines": "ess_numerique",
                "methode_jointure": "siren_direct",
                "confiance_jointure": 1.0,
                "denomination": "AlphaNum",
                "naf": "62.01Z",
                "naf_numerique": True,
                "tags_numerique": "dev_logiciel",
                # Champs vides à enrichir
                "categorie_juridique": np.nan,
                "tranche_effectifs": np.nan,
                "famille_ess": np.nan,
                "nom_usage": np.nan,
            },
            {
                "siren": "222222222",
                "sources": "ess_france",
                "origines": "cate_juridique",
                "methode_jointure": "siren_direct",
                "confiance_jointure": 1.0,
                "denomination": "",  # vide → sera enrichi
                "naf": np.nan,      # vide → sera enrichi
                "naf_numerique": False,
                "tags_numerique": np.nan,
                "categorie_juridique": np.nan,
                "tranche_effectifs": np.nan,
                "famille_ess": np.nan,
                "nom_usage": np.nan,
            },
            {
                "siren": "333333333",
                "sources": "tiers_lieux",
                "origines": "tiers_lieux",
                "methode_jointure": "scoring",
                "confiance_jointure": 0.80,
                "denomination": "GammaFab",  # déjà rempli → pas écrasé
                "naf": np.nan,
                "naf_numerique": False,
                "tags_numerique": np.nan,
                "categorie_juridique": np.nan,
                "tranche_effectifs": np.nan,
                "famille_ess": np.nan,
                "nom_usage": np.nan,
            },
            {
                "siren": "999999999",  # pas dans SIRENE
                "sources": "tiers_lieux",
                "origines": "tiers_lieux",
                "methode_jointure": "scoring",
                "confiance_jointure": 0.75,
                "denomination": "NotInSirene",
                "naf": np.nan,
                "naf_numerique": False,
                "tags_numerique": np.nan,
                "categorie_juridique": np.nan,
                "tranche_effectifs": np.nan,
                "famille_ess": np.nan,
                "nom_usage": np.nan,
            },
        ]

        consolidated_path = tmp_path / "x_consolidated.csv"
        pd.DataFrame(consolidated_rows).to_csv(
            consolidated_path, index=False
        )

        output_path = tmp_path / "x_enriched.csv"

        # --- Exécuter le pipeline ---
        df = main(
            consolidated_path=consolidated_path,
            sirene_path=parquet_path,
            output_path=output_path,
        )

        # --- Assertions ---
        assert len(df) == 4
        assert output_path.exists()

        # --- 111111111 : naf déjà rempli, categorie_juridique enrichie ---
        r1 = df[df["siren"] == "111111111"].iloc[0]
        assert r1["naf"] == "62.01Z"  # inchangé
        assert pd.isna(r1["naf_enrich_source"])  # pas enrichi car déjà rempli
        assert r1["denomination"] == "AlphaNum"  # inchangé
        assert r1["categorie_juridique"] == "9220"  # enrichi
        assert r1["categorie_juridique_enrich_source"] == "sirene"
        assert r1["tranche_effectifs"] == "12"
        assert r1["famille_ess"] == "association"  # re-dérivé
        assert r1["siret_siege"] == "11111111100012"
        assert r1["est_active"] == True
        assert r1["rna"] == "W751234567"

        # --- 222222222 : tout vide → enrichi depuis SIRENE (période récente) ---
        r2 = df[df["siren"] == "222222222"].iloc[0]
        assert r2["denomination"] == "BETA COOP"  # enrichi (période 2024)
        assert r2["naf"] == "62.02A"  # enrichi
        assert r2["naf_enrich_source"] == "sirene"
        assert r2["categorie_juridique"] == "5458"
        assert r2["tranche_effectifs"] == "22"  # période récente
        assert r2["naf_numerique"] == True  # re-dérivé
        assert r2["tags_numerique"] == "conseil_it"  # re-dérivé
        assert r2["famille_ess"] == "cooperative"  # re-dérivé
        assert r2["siret_siege"] == "22222222200025"

        # --- 333333333 : denomination gardée, le reste enrichi ---
        r3 = df[df["siren"] == "333333333"].iloc[0]
        assert r3["denomination"] == "GammaFab"  # gardé (pas écrasé)
        assert pd.isna(r3["denomination_enrich_source"])
        assert r3["naf"] == "62.03Z"  # enrichi
        assert r3["categorie_juridique"] == "9210"
        assert r3["famille_ess"] == "association"  # re-dérivé
        assert r3["est_active"] == False  # C → False

        # --- 999999999 : pas dans SIRENE → rien enrichi ---
        r4 = df[df["siren"] == "999999999"].iloc[0]
        assert r4["denomination"] == "NotInSirene"  # inchangé
        assert pd.isna(r4["naf"])  # toujours vide
        assert pd.isna(r4["categorie_juridique"])
        assert pd.isna(r4.get("naf_enrich_source", np.nan))

    def test_output_has_traceability_columns(self, tmp_path, parquet_path):
        """Vérifie que les colonnes de traçabilité sont présentes."""
        consolidated_path = tmp_path / "x_consolidated.csv"
        pd.DataFrame([{
            "siren": "111111111",
            "denomination": np.nan,
            "naf": np.nan,
        }]).to_csv(consolidated_path, index=False)

        output_path = tmp_path / "x_enriched.csv"
        df = main(
            consolidated_path=consolidated_path,
            sirene_path=parquet_path,
            output_path=output_path,
        )

        # Vérifier les colonnes de traçabilité pour chaque champ du mapping
        for _, target_col, _ in SIRENE_FIELD_MAPPING:
            assert f"{target_col}_enrich_source" in df.columns, (
                f"Colonne {target_col}_enrich_source manquante"
            )
            assert f"{target_col}_enrich_method" in df.columns, (
                f"Colonne {target_col}_enrich_method manquante"
            )

        # siret_siege aussi
        assert "siret_siege_enrich_source" in df.columns
        assert "siret_siege_enrich_method" in df.columns
