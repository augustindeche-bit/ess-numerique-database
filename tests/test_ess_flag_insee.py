"""Tests unitaires pour 04_ess_flag_insee.py — pipeline candidat flag ESS INSEE."""

import importlib.util
import sys
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

# ── Import du module (repertoire 01_Candidates commence par un chiffre) ──

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

_spec = importlib.util.spec_from_file_location(
    "ess_flag_insee",
    ROOT / "src" / "01_Candidates" / "04_ess_flag_insee.py",
)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

main = _mod.main
CODES_NAF_NUMERIQUE = _mod.CODES_NAF_NUMERIQUE


# ── Helpers ──


def _make_parquet(path: Path, rows: list[dict]) -> Path:
    """Cree un mini fichier parquet a partir d'une liste de dicts."""
    df = pd.DataFrame(rows)
    table = pa.Table.from_pandas(df)
    pq.write_table(table, path)
    return path


# ── Donnees simulees ──

SAMPLE_ROWS = [
    {
        "siren": "111111111",
        "denominationUniteLegale": "AlphaESS",
        "nomUniteLegale": "AlphaUsage",
        "activitePrincipaleUniteLegale": "62.01Z",
        "economieSocialeSolidaireUniteLegale": "O",
        "categorieJuridiqueUniteLegale": 9220,
        "trancheEffectifsUniteLegale": "12",
    },
    {
        "siren": "222222222",
        "denominationUniteLegale": "BetaNonESS",
        "nomUniteLegale": "BetaUsage",
        "activitePrincipaleUniteLegale": "62.01Z",
        "economieSocialeSolidaireUniteLegale": "N",
        "categorieJuridiqueUniteLegale": 5458,
        "trancheEffectifsUniteLegale": "03",
    },
    {
        "siren": "333333333",
        "denominationUniteLegale": "GammaESSNonNAF",
        "nomUniteLegale": "GammaUsage",
        "activitePrincipaleUniteLegale": "01.11Z",
        "economieSocialeSolidaireUniteLegale": "O",
        "categorieJuridiqueUniteLegale": 9210,
        "trancheEffectifsUniteLegale": "00",
    },
]


@pytest.fixture
def parquet_path(tmp_path):
    return _make_parquet(tmp_path / "stock.parquet", SAMPLE_ROWS)


@pytest.fixture
def output_path(tmp_path):
    return tmp_path / "output.csv"


# ══════════════════════════════════════════════
# Tests
# ══════════════════════════════════════════════


class TestFiltering:
    def test_only_ess_and_naf_kept(self, parquet_path, output_path):
        """Seules les lignes ESS=O ET NAF numerique doivent etre gardees."""
        main(sirene_path=parquet_path, output_path=output_path)
        df = pd.read_csv(output_path, dtype={"siren": str})
        assert len(df) == 1
        assert df.iloc[0]["siren"] == "111111111"

    def test_non_ess_excluded(self, parquet_path, output_path):
        """Les lignes avec ESS != 'O' sont exclues."""
        main(sirene_path=parquet_path, output_path=output_path)
        df = pd.read_csv(output_path, dtype={"siren": str})
        assert "222222222" not in df["siren"].values

    def test_ess_non_naf_excluded(self, parquet_path, output_path):
        """Les lignes ESS=O mais NAF hors numerique sont exclues."""
        main(sirene_path=parquet_path, output_path=output_path)
        df = pd.read_csv(output_path, dtype={"siren": str})
        assert "333333333" not in df["siren"].values


class TestColumns:
    def test_expected_columns(self, parquet_path, output_path):
        main(sirene_path=parquet_path, output_path=output_path)
        df = pd.read_csv(output_path, dtype={"siren": str})
        expected = {
            "siren", "denominationUniteLegale", "nomUniteLegale",
            "activitePrincipaleUniteLegale",
            "economieSocialeSolidaireUniteLegale",
            "categorieJuridiqueUniteLegale",
            "trancheEffectifsUniteLegale",
            "sources", "methode_jointure",
        }
        assert expected.issubset(set(df.columns))

    def test_sources_value(self, parquet_path, output_path):
        main(sirene_path=parquet_path, output_path=output_path)
        df = pd.read_csv(output_path, dtype={"siren": str})
        assert (df["sources"] == "sirene").all()

    def test_methode_jointure_value(self, parquet_path, output_path):
        main(sirene_path=parquet_path, output_path=output_path)
        df = pd.read_csv(output_path, dtype={"siren": str})
        assert (df["methode_jointure"] == "siren_direct").all()


class TestAllNafCodes:
    def test_all_10_naf_codes_accepted(self, tmp_path):
        """Les 10 codes NAF numeriques doivent tous etre acceptes."""
        rows = [
            {
                "siren": f"{i:09d}",
                "denominationUniteLegale": f"Org{i}",
                "nomUniteLegale": f"Usage{i}",
                "activitePrincipaleUniteLegale": code,
                "economieSocialeSolidaireUniteLegale": "O",
                "categorieJuridiqueUniteLegale": 9220,
                "trancheEffectifsUniteLegale": "00",
            }
            for i, code in enumerate(CODES_NAF_NUMERIQUE, start=1)
        ]
        parquet = _make_parquet(tmp_path / "all_naf.parquet", rows)
        output = tmp_path / "all_naf_out.csv"
        main(sirene_path=parquet, output_path=output)
        df = pd.read_csv(output, dtype={"siren": str})
        assert len(df) == 10
        assert set(df["activitePrincipaleUniteLegale"]) == set(CODES_NAF_NUMERIQUE)
