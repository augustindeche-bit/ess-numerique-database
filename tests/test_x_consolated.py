"""Tests unitaires pour x_consolated.py — consolidation des fichiers candidats."""

import importlib.util
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

# ── Import du module (répertoire 01_Candidates commence par un chiffre) ──

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

_spec = importlib.util.spec_from_file_location(
    "x_consolated",
    ROOT / "src" / "01_Candidates" / "x_consolated.py",
)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

load_and_normalize = _mod.load_and_normalize
merge_sources = _mod.merge_sources
resolve_attribute = _mod.resolve_attribute
best_methode = _mod.best_methode
consolidate_group = _mod.consolidate_group
enrich = _mod.enrich
ATTRS_MERGEABLE = _mod.ATTRS_MERGEABLE


# ── Helpers ──


def _write_csv(path: Path, rows: list[dict]) -> Path:
    pd.DataFrame(rows).to_csv(path, index=False)
    return path


def _make_group(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows)


# ── Données simulées (5 SIREN couvrant tous les cas) ──

ESS_NUM_ROWS = [
    {
        "siren": "111111111",
        "denominationUniteLegale": "AlphaNum",
        "activitePrincipaleUniteLegale": "62.01Z",
        "sources": "sirene",
        "methode_jointure": "siren_direct",
    },
    {
        "siren": "222222222",
        "denominationUniteLegale": "BetaCoop",
        "activitePrincipaleUniteLegale": "62.02A",
        "sources": "ess_france",
        "methode_jointure": "siren_direct",
    },
    {
        "siren": "333333333",
        "denominationUniteLegale": "GammaLab",
        "activitePrincipaleUniteLegale": "62.03Z",
        "sources": "sirene",
        "methode_jointure": "siren_direct",
        "confiance_jointure": 1.0,
    },
]

CATE_JURID_ROWS = [
    {
        "siren": "222222222",
        "denominationUniteLegale": "BetaCoop",
        "activitePrincipaleUniteLegale": "62.02A",
        "categorieJuridiqueUniteLegale": "5458",
        "sources": "ess_france",
        "methode_jointure": "siren_direct",
        "group_naf": "group_naf_2",
        "group_cate": "group_cate_2",
    },
    {
        "siren": "333333333",
        "denominationUniteLegale": "GammaLab-Alt",
        "activitePrincipaleUniteLegale": "62.03Z",
        "categorieJuridiqueUniteLegale": "9220",
        "sources": "ess_france|esus",
        "methode_jointure": "scoring",
        "confiance_jointure": 0.85,
        "group_naf": "group_naf_2",
        "group_cate": "group_cate_1",
    },
    {
        "siren": "444444444",
        "denominationUniteLegale": "DeltaCoop",
        "activitePrincipaleUniteLegale": "58.29A",
        "categorieJuridiqueUniteLegale": "5458",
        "sources": "scop_scic",
        "methode_jointure": "siren_direct",
        "group_naf": "group_naf_1",
        "group_cate": "group_cate_2",
    },
]

TIERS_LIEUX_ROWS = [
    {
        "siren": "333333333",
        "NOM": "GammaFab",
        "NOM_STRC_PORT": "PortGamma",
        "sources": "tiers_lieux",
        "methode_jointure": "scoring",
        "confiance_jointure": 0.72,
    },
    {
        "siren": "555555555",
        "NOM": "EpsilonFab",
        "CODPOST": "75013",
        "INTERNET": "https://epsilon.org",
        "sources": "tiers_lieux",
        "methode_jointure": "scoring",
        "confiance_jointure": 0.80,
    },
]


ESS_FLAG_INSEE_ROWS = [
    {
        "siren": "111111111",
        "denominationUniteLegale": "AlphaNum",
        "nomUniteLegale": "AlphaUsage",
        "activitePrincipaleUniteLegale": "62.01Z",
        "categorieJuridiqueUniteLegale": "9220",
        "trancheEffectifsUniteLegale": "12",
        "economieSocialeSolidaireUniteLegale": "O",
        "sources": "sirene",
        "methode_jointure": "siren_direct",
    },
    {
        "siren": "666666666",
        "denominationUniteLegale": "ZetaESS",
        "nomUniteLegale": "ZetaUsage",
        "activitePrincipaleUniteLegale": "63.11Z",
        "categorieJuridiqueUniteLegale": "9210",
        "trancheEffectifsUniteLegale": "03",
        "economieSocialeSolidaireUniteLegale": "O",
        "sources": "sirene",
        "methode_jointure": "siren_direct",
    },
]


def _make_config(csv_dir: Path, origin: str) -> dict:
    """Construit un dict config identique à INPUTS pour une origine donnée."""
    configs = {
        "ess_x_naf": {
            "path": csv_dir / "01_ess_x_naf.csv",
            "rename": {
                "denominationUniteLegale": "denomination",
                "activitePrincipaleUniteLegale": "naf",
            },
            "defaults": {},
        },
        "cate_x_naf": {
            "path": csv_dir / "02_cate_x_naf.csv",
            "rename": {
                "denominationUniteLegale": "denomination",
                "nomUniteLegale": "nom_usage",
                "activitePrincipaleUniteLegale": "naf",
                "trancheEffectifsUniteLegale": "tranche_effectifs",
                "categorieJuridiqueUniteLegale": "categorie_juridique",
            },
            "defaults": {},
        },
        "tiers_lieux": {
            "path": csv_dir / "03_tiers_lieux.csv",
            "rename": {
                "NOM": "denomination",
                "NOM_STRC_PORT": "nom_structure_porteuse",
                "CODPOST": "code_postal",
                "VILLE": "commune",
                "INTERNET": "site_web",
            },
            "defaults": {},
        },
        "ess_flag_insee": {
            "path": csv_dir / "04_ess_flag_insee.csv",
            "rename": {
                "denominationUniteLegale": "denomination",
                "nomUniteLegale": "nom_usage",
                "activitePrincipaleUniteLegale": "naf",
                "categorieJuridiqueUniteLegale": "categorie_juridique",
                "trancheEffectifsUniteLegale": "tranche_effectifs",
            },
            "defaults": {},
        },
    }
    return configs[origin]


@pytest.fixture
def csv_dir(tmp_path):
    """Crée les 4 fichiers CSV simulés dans un répertoire temporaire."""
    _write_csv(tmp_path / "01_ess_x_naf.csv", ESS_NUM_ROWS)
    _write_csv(tmp_path / "02_cate_x_naf.csv", CATE_JURID_ROWS)
    _write_csv(tmp_path / "03_tiers_lieux.csv", TIERS_LIEUX_ROWS)
    _write_csv(tmp_path / "04_ess_flag_insee.csv", ESS_FLAG_INSEE_ROWS)
    return tmp_path


# ══════════════════════════════════════════════
# Tests : load_and_normalize
# ══════════════════════════════════════════════


class TestLoadAndNormalize:
    def test_rename_columns(self, csv_dir):
        df = load_and_normalize("ess_x_naf", _make_config(csv_dir, "ess_x_naf"))
        assert "denomination" in df.columns
        assert "naf" in df.columns
        assert "denominationUniteLegale" not in df.columns

    def test_origin_added(self, csv_dir):
        df = load_and_normalize("ess_x_naf", _make_config(csv_dir, "ess_x_naf"))
        assert "_origin" in df.columns
        assert (df["_origin"] == "ess_x_naf").all()

    def test_siren_zero_padding(self, tmp_path):
        _write_csv(tmp_path / "short.csv", [{"siren": "12345"}])
        config = {"path": tmp_path / "short.csv", "rename": {}, "defaults": {}}
        df = load_and_normalize("test", config)
        assert df.iloc[0]["siren"] == "000012345"

    def test_siren_float_cleanup(self, tmp_path):
        _write_csv(tmp_path / "float.csv", [{"siren": "333333333.0"}])
        config = {"path": tmp_path / "float.csv", "rename": {}, "defaults": {}}
        df = load_and_normalize("test", config)
        assert df.iloc[0]["siren"] == "333333333"

    def test_invalid_siren_dropped(self, tmp_path):
        _write_csv(
            tmp_path / "bad.csv",
            [
                {"siren": "ABC", "x": "bad"},
                {"siren": "111111111", "x": "good"},
            ],
        )
        config = {"path": tmp_path / "bad.csv", "rename": {}, "defaults": {}}
        df = load_and_normalize("test", config)
        assert len(df) == 1
        assert df.iloc[0]["siren"] == "111111111"

    def test_missing_file_returns_empty(self, tmp_path):
        config = {"path": tmp_path / "nope.csv", "rename": {}, "defaults": {}}
        df = load_and_normalize("test", config)
        assert df.empty

    def test_defaults_applied(self, tmp_path):
        _write_csv(tmp_path / "d.csv", [{"siren": "111111111"}])
        config = {"path": tmp_path / "d.csv", "rename": {}, "defaults": {"extra": "val"}}
        df = load_and_normalize("test", config)
        assert "extra" in df.columns
        assert df.iloc[0]["extra"] == "val"


# ══════════════════════════════════════════════
# Tests : merge_sources
# ══════════════════════════════════════════════


class TestMergeSources:
    def test_single_source(self):
        group = _make_group([{"sources": "sirene"}])
        assert merge_sources(group) == "sirene"

    def test_union_multi(self):
        group = _make_group([{"sources": "sirene"}, {"sources": "ess_france|esus"}])
        assert merge_sources(group) == "ess_france|esus|sirene"

    def test_deduplication(self):
        group = _make_group([{"sources": "sirene|ess_france"}, {"sources": "ess_france"}])
        assert merge_sources(group) == "ess_france|sirene"

    def test_no_sources_column(self):
        group = _make_group([{"other": "x"}])
        assert merge_sources(group) == ""

    def test_all_nan(self):
        group = _make_group([{"sources": np.nan}, {"sources": np.nan}])
        assert merge_sources(group) == ""


# ══════════════════════════════════════════════
# Tests : resolve_attribute
# ══════════════════════════════════════════════


class TestResolveAttribute:
    def test_concordant(self):
        group = _make_group([
            {"_origin": "a", "denomination": "Same"},
            {"_origin": "b", "denomination": "Same"},
        ])
        result = resolve_attribute(group, "denomination")
        assert result == {"denomination": "Same"}

    def test_divergent(self):
        group = _make_group([
            {"_origin": "a", "denomination": "Val_A"},
            {"_origin": "b", "denomination": "Val_B"},
        ])
        result = resolve_attribute(group, "denomination")
        assert result["denomination"] == "Val_A"
        assert result["denomination__a"] == "Val_A"
        assert result["denomination__b"] == "Val_B"

    def test_column_absent(self):
        group = _make_group([{"_origin": "a", "other": "x"}])
        assert resolve_attribute(group, "denomination") == {}

    def test_all_nan(self):
        group = _make_group([
            {"_origin": "a", "denomination": np.nan},
            {"_origin": "b", "denomination": np.nan},
        ])
        assert resolve_attribute(group, "denomination") == {}

    def test_partial_nan(self):
        group = _make_group([
            {"_origin": "a", "denomination": "OnlyOne"},
            {"_origin": "b", "denomination": np.nan},
        ])
        result = resolve_attribute(group, "denomination")
        assert result == {"denomination": "OnlyOne"}


# ══════════════════════════════════════════════
# Tests : best_methode
# ══════════════════════════════════════════════


class TestBestMethode:
    def test_single(self):
        group = _make_group([{"methode_jointure": "scoring"}])
        assert best_methode(group) == "scoring"

    def test_priority_siren_direct_over_scoring(self):
        group = _make_group([
            {"methode_jointure": "scoring"},
            {"methode_jointure": "siren_direct"},
        ])
        assert best_methode(group) == "siren_direct"

    def test_no_column(self):
        group = _make_group([{"other": "x"}])
        assert best_methode(group) is None

    def test_all_nan(self):
        group = _make_group([{"methode_jointure": np.nan}])
        assert best_methode(group) is None


# ══════════════════════════════════════════════
# Tests : consolidate_group
# ══════════════════════════════════════════════


class TestConsolidateGroup:
    def test_single_source(self):
        group = _make_group([{
            "_origin": "ess_x_naf",
            "sources": "sirene",
            "denomination": "Alpha",
            "naf": "62.01Z",
            "methode_jointure": "siren_direct",
        }])
        rec = consolidate_group("111111111", group)
        assert rec["siren"] == "111111111"
        assert rec["origines"] == "ess_x_naf"
        assert rec["denomination"] == "Alpha"
        assert rec["confiance_jointure"] == 1.0  # default pour siren_direct

    def test_multi_source_concordant(self):
        group = _make_group([
            {
                "_origin": "ess_x_naf",
                "sources": "ess_france",
                "denomination": "Beta",
                "naf": "62.02A",
                "methode_jointure": "siren_direct",
            },
            {
                "_origin": "cate_x_naf",
                "sources": "ess_france",
                "denomination": "Beta",
                "naf": "62.02A",
                "methode_jointure": "siren_direct",
                "categorie_juridique": "5458",
            },
        ])
        rec = consolidate_group("222222222", group)
        assert rec["denomination"] == "Beta"
        assert "denomination__ess_x_naf" not in rec
        assert rec["categorie_juridique"] == "5458"

    def test_multi_source_divergent(self):
        group = _make_group([
            {
                "_origin": "ess_x_naf",
                "sources": "sirene",
                "denomination": "GammaLab",
                "naf": "62.03Z",
                "methode_jointure": "siren_direct",
                "confiance_jointure": 1.0,
            },
            {
                "_origin": "cate_x_naf",
                "sources": "ess_france|esus",
                "denomination": "GammaLab-Alt",
                "naf": "62.03Z",
                "methode_jointure": "scoring",
                "confiance_jointure": 0.85,
            },
            {
                "_origin": "tiers_lieux",
                "sources": "tiers_lieux",
                "denomination": "GammaFab",
                "methode_jointure": "scoring",
                "confiance_jointure": 0.72,
                "nom_structure_porteuse": "PortGamma",
            },
        ])
        rec = consolidate_group("333333333", group)
        assert rec["denomination__ess_x_naf"] == "GammaLab"
        assert rec["denomination__cate_x_naf"] == "GammaLab-Alt"
        assert rec["denomination__tiers_lieux"] == "GammaFab"
        assert rec["methode_jointure"] == "siren_direct"
        assert rec["confiance_jointure"] == 1.0
        assert rec["nom_structure_porteuse"] == "PortGamma"

    def test_confiance_max(self):
        group = _make_group([
            {"_origin": "a", "methode_jointure": "scoring", "confiance_jointure": 0.6},
            {"_origin": "b", "methode_jointure": "scoring", "confiance_jointure": 0.9},
        ])
        rec = consolidate_group("999999999", group)
        assert rec["confiance_jointure"] == 0.9

    def test_confiance_default_siren_direct(self):
        group = _make_group([{"_origin": "a", "methode_jointure": "siren_direct"}])
        rec = consolidate_group("999999999", group)
        assert rec["confiance_jointure"] == 1.0


# ══════════════════════════════════════════════
# Tests : enrich
# ══════════════════════════════════════════════


class TestEnrich:
    def test_famille_association(self):
        df = pd.DataFrame([{"categorie_juridique": "9220", "naf": "01.11Z"}])
        df = enrich(df)
        assert df.iloc[0]["famille_ess"] == "association"

    def test_famille_cooperative(self):
        df = pd.DataFrame([{"categorie_juridique": "5458", "naf": "01.11Z"}])
        df = enrich(df)
        assert df.iloc[0]["famille_ess"] == "cooperative"

    def test_famille_nan(self):
        df = pd.DataFrame([{"categorie_juridique": np.nan, "naf": "01.11Z"}])
        df = enrich(df)
        assert pd.isna(df.iloc[0]["famille_ess"])

    def test_famille_unknown_code(self):
        df = pd.DataFrame([{"categorie_juridique": "9999", "naf": "01.11Z"}])
        df = enrich(df)
        assert pd.isna(df.iloc[0]["famille_ess"])

    def test_naf_numerique_true(self):
        df = pd.DataFrame([{"categorie_juridique": np.nan, "naf": "62.01Z"}])
        df = enrich(df)
        assert df.iloc[0]["naf_numerique"] == True
        assert df.iloc[0]["tags_numerique"] == "dev_logiciel"

    def test_naf_numerique_false(self):
        df = pd.DataFrame([{"categorie_juridique": np.nan, "naf": "01.11Z"}])
        df = enrich(df)
        assert df.iloc[0]["naf_numerique"] == False
        assert pd.isna(df.iloc[0]["tags_numerique"])

    def test_categorie_juridique_float_string(self):
        df = pd.DataFrame([{"categorie_juridique": "5458.0", "naf": "01.11Z"}])
        df = enrich(df)
        assert df.iloc[0]["famille_ess"] == "cooperative"


# ══════════════════════════════════════════════
# Test d'intégration : pipeline complet
# ══════════════════════════════════════════════


class TestFullPipeline:
    def test_full_pipeline(self, csv_dir):
        # 1. Chargement
        frames = []
        for origin in ("ess_x_naf", "cate_x_naf", "tiers_lieux", "ess_flag_insee"):
            df = load_and_normalize(origin, _make_config(csv_dir, origin))
            if not df.empty:
                frames.append(df)

        df_all = pd.concat(frames, ignore_index=True)

        # 2. Consolidation par SIREN
        records = []
        for siren, group in df_all.groupby("siren"):
            records.append(consolidate_group(siren, group))

        df_out = pd.DataFrame(records)

        # 3. Enrichissement
        df_out = enrich(df_out)

        # --- Assertions globales ---
        assert len(df_out) == 6

        sirens = set(df_out["siren"])
        assert sirens == {
            "111111111", "222222222", "333333333",
            "444444444", "555555555", "666666666",
        }

        # Colonnes obligatoires remplies pour tous
        for _, row in df_out.iterrows():
            assert row["sources"], f"sources vide pour {row['siren']}"
            assert row["origines"], f"origines vide pour {row['siren']}"

        # --- 111111111 : ess_x_naf + ess_flag_insee ---
        r1 = df_out[df_out["siren"] == "111111111"].iloc[0]
        assert "ess_x_naf" in r1["origines"]
        assert "ess_flag_insee" in r1["origines"]
        assert r1["methode_jointure"] == "siren_direct"
        assert r1["confiance_jointure"] == 1.0
        assert r1["naf_numerique"] == True
        assert r1["tags_numerique"] == "dev_logiciel"

        # --- 222222222 : 2 sources, denomination concordante ---
        r2 = df_out[df_out["siren"] == "222222222"].iloc[0]
        assert "ess_x_naf" in r2["origines"]
        assert "cate_x_naf" in r2["origines"]
        assert r2["denomination"] == "BetaCoop"
        # Pas de conflit denomination
        assert pd.isna(r2.get("denomination__ess_x_naf", np.nan))
        assert r2["famille_ess"] == "cooperative"

        # --- 333333333 : 3 sources, denomination divergente ---
        r3 = df_out[df_out["siren"] == "333333333"].iloc[0]
        assert "ess_x_naf" in r3["origines"]
        assert "cate_x_naf" in r3["origines"]
        assert "tiers_lieux" in r3["origines"]
        # Conflit denomination detecte
        assert "denomination__ess_x_naf" in df_out.columns
        assert r3["denomination__ess_x_naf"] == "GammaLab"
        assert r3["denomination__cate_x_naf"] == "GammaLab-Alt"
        assert r3["denomination__tiers_lieux"] == "GammaFab"
        assert r3["methode_jointure"] == "siren_direct"
        assert r3["confiance_jointure"] == 1.0
        assert r3["famille_ess"] == "association"
        assert r3["naf_numerique"] == True
        assert r3["nom_structure_porteuse"] == "PortGamma"

        # --- 444444444 : source unique cate_x_naf ---
        r4 = df_out[df_out["siren"] == "444444444"].iloc[0]
        assert r4["origines"] == "cate_x_naf"
        assert r4["famille_ess"] == "cooperative"
        assert r4["naf_numerique"] == True
        assert r4["tags_numerique"] == "edition_logiciel"

        # --- 555555555 : source unique tiers_lieux ---
        r5 = df_out[df_out["siren"] == "555555555"].iloc[0]
        assert r5["origines"] == "tiers_lieux"
        # code_postal transits par CSV comme float -> "75013.0" apres str()
        assert r5["code_postal"] in ("75013", "75013.0")
        assert r5["site_web"] == "https://epsilon.org"
        assert r5["confiance_jointure"] == 0.80
        assert r5["naf_numerique"] == False

        # --- 666666666 : source unique ess_flag_insee ---
        r6 = df_out[df_out["siren"] == "666666666"].iloc[0]
        assert r6["origines"] == "ess_flag_insee"
        assert r6["denomination"] == "ZetaESS"
        assert r6["naf_numerique"] == True

        # --- group_naf / group_cate pass-through ---
        assert r2["group_naf"] == "group_naf_2"
        assert r2["group_cate"] == "group_cate_2"
        assert r4["group_naf"] == "group_naf_1"
        assert r4["group_cate"] == "group_cate_2"
