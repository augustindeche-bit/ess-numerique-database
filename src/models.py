"""Modèles de données — Base élargie et types associés."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from enum import Enum
from typing import Optional

# ──────────────────────────────────────────────
# Enums — vocabulaire contrôlé
# ──────────────────────────────────────────────

class FamilleESS(str, Enum):
    """Famille juridique ESS."""
    ASSOCIATION = "association"
    COOPERATIVE = "cooperative"
    MUTUELLE = "mutuelle"
    FONDATION = "fondation"
    ENTREPRISE_ESS = "entreprise_ess"

class MethodeJointure(str, Enum):
    """Méthode utilisée pour résoudre le SIREN."""
    SIREN_DIRECT = "siren_direct"
    VIA_RNA = "via_rna"
    API_SIRENE = "api_sirene"
    SCORING = "scoring"
    SCRAPING = "scraping"
    MANUEL = "manuel"

class TagNumerique(str, Enum):
    """Tags d'activité numérique (vocabulaire contrôlé)."""

    DEV_LOGICIEL = "dev_logiciel"
    CONSEIL_IT = "conseil_it"
    HEBERGEMENT = "hebergement"
    PORTAIL_WEB = "portail_web"
    EDITION_LOGICIEL = "edition_logiciel"
    EDITION_JEUX = "edition_jeux"
    FABLAB = "fablab"
    MEDIATION_NUMERIQUE = "mediation_numerique"
    FORMATION_NUMERIQUE = "formation_numerique"
    FAI_ASSOCIATIF = "fai_associatif"
    OPEN_DATA = "open_data"


class Source(str, Enum):
    """Identifiants des sources de données."""
    SIRENE = "sirene" ## StockUniteLegale_utf8.parquet
    ESS_FRANCE = "ess_france" ## entreprisesess.csv
    ESUS = "esus"  ##  liste_nationale_esus.xlsx
    SCOP_SCIC = "scop_scic"
    TIERS_LIEUX = "tiers_lieux" ## bdftl-2023-01-fiche-identite.csv bdftl-2023-04-activites.csv
    FFDN = "ffdn"
    MEDNUM = "mednum"
    OPENDATAFRANCE = "opendatafrance"
    LABEL_INR = "label_inr"
    LABEL_BCORP = "label_bcorp"
    LABEL_LUCIE = "label_lucie"
    SOGA = "soga"
    AVISE = "avise"
    NGI = "ngi"  ##csvExportNGI.csv

# ──────────────────────────────────────────────
# Mapping NAF → tag numérique
# ──────────────────────────────────────────────

NAF_NUMERIQUE: dict[str, TagNumerique] = {
    "62.01Z": TagNumerique.DEV_LOGICIEL,
    "62.02A": TagNumerique.CONSEIL_IT,
    "62.02B": TagNumerique.CONSEIL_IT,
    "62.03Z": TagNumerique.HEBERGEMENT,
    "63.11Z": TagNumerique.HEBERGEMENT,
    "63.12Z": TagNumerique.PORTAIL_WEB,
    "58.21Z": TagNumerique.EDITION_JEUX,
    "58.29A": TagNumerique.EDITION_LOGICIEL,
    "58.29B": TagNumerique.EDITION_LOGICIEL,
    "58.29C": TagNumerique.EDITION_LOGICIEL,
}

GROUP_NAF: dict[str, str] = {
    "62.01Z": "group_naf_1",
    "58.29A": "group_naf_1",
    "58.29B": "group_naf_1",
    "58.29C": "group_naf_1",
    "63.11Z": "group_naf_1",
    "62.02A": "group_naf_2",
    "62.02B": "group_naf_2",
    "62.03Z": "group_naf_2",
    "63.12Z": "group_naf_2",
    "58.21Z": "group_naf_2",
}

# ──────────────────────────────────────────────
# Mapping catégorie juridique → famille ESS
# ──────────────────────────────────────────────

CATEGORIE_JURIDIQUE_FAMILLE: dict[str, FamilleESS] = {
    # Associations
    "9210": FamilleESS.ASSOCIATION,
    "9220": FamilleESS.ASSOCIATION,
    "9221": FamilleESS.ASSOCIATION,
    "9222": FamilleESS.ASSOCIATION,
    "9230": FamilleESS.ASSOCIATION,
    "9240": FamilleESS.ASSOCIATION,
    "9260": FamilleESS.ASSOCIATION,
    # Coopératives
    "5458": FamilleESS.COOPERATIVE,
    "5547": FamilleESS.COOPERATIVE,
    "5558": FamilleESS.COOPERATIVE,
    "5560": FamilleESS.COOPERATIVE,
    "6316": FamilleESS.COOPERATIVE,
    "6317": FamilleESS.COOPERATIVE,
    "6318": FamilleESS.COOPERATIVE,
    # Mutuelles
    "8210": FamilleESS.MUTUELLE,
    "8250": FamilleESS.MUTUELLE,
    "8290": FamilleESS.MUTUELLE,
    # Fondations
    "9300": FamilleESS.FONDATION,
    "9310": FamilleESS.FONDATION,
    "9900": FamilleESS.FONDATION,
}

GROUP_CATE: dict[str, str] = {
    "9220": "group_cate_1",
    "6316": "group_cate_1",
    "6317": "group_cate_1",
    "6318": "group_cate_1",
    "9300": "group_cate_1",
    "9210": "group_cate_2",
    "9221": "group_cate_2",
    "9222": "group_cate_2",
    "9230": "group_cate_2",
    "9240": "group_cate_2",
    "9260": "group_cate_2",
    "5458": "group_cate_2",
    "5547": "group_cate_2",
    "5558": "group_cate_2",
    "5560": "group_cate_2",
    "8210": "group_cate_2",
    "8250": "group_cate_2",
    "8290": "group_cate_2",
    "9310": "group_cate_2",
    "9900": "group_cate_2",
}


# ──────────────────────────────────────────────
# Dataclass principale
# ──────────────────────────────────────────────


@dataclass
class OrganisationESS:
    """Une organisation dans la base élargie.

    Contrainte : une seule entrée par SIREN.
    Les sources multiples sont agrégées dans `sources` et `tags_numerique`.
    """

    # --- Identifiants ---
    siren: str
    siret_siege: Optional[str] = None
    rna: Optional[str] = None
    tva_intra: Optional[str] = None

    # --- Identité ---
    denomination: str = ""
    nom_usage: Optional[str] = None
    sigle: Optional[str] = None

    # --- Classification ESS ---
    categorie_juridique: str = ""
    categorie_juridique_libelle: Optional[str] = None
    famille_ess: Optional[FamilleESS] = None
    agrement_esus: bool = False
    flag_ess_sirene: bool = False
    flag_ess_liste: bool = False

    # --- Classification numérique ---
    naf: str = ""
    naf_libelle: Optional[str] = None
    naf_numerique: bool = False
    tags_numerique: set[TagNumerique] = field(default_factory=set)

    # --- Localisation ---
    code_postal: Optional[str] = None
    commune: Optional[str] = None
    departement: Optional[str] = None
    region: Optional[str] = None

    # --- Activité & taille ---
    tranche_effectifs: Optional[str] = None
    date_creation: Optional[date] = None
    est_active: Optional[bool] = None
    site_web: Optional[str] = None

    # --- Provenance & qualité ---
    sources: set[Source] = field(default_factory=set)
    methode_jointure: MethodeJointure = MethodeJointure.SIREN_DIRECT
    confiance_jointure: Optional[float] = None
    date_integration: date = field(default_factory=date.today)

    def __post_init__(self) -> None:
        if len(self.siren) != 9 or not self.siren.isdigit():
            raise ValueError(f"SIREN invalide : '{self.siren}' (9 chiffres attendus)")

        if self.siret_siege and (
            len(self.siret_siege) != 14 or not self.siret_siege.isdigit()
        ):
            raise ValueError(
                f"SIRET invalide : '{self.siret_siege}' (14 chiffres attendus)"
            )

        if self.rna and (
            len(self.rna) != 10 or not self.rna[0] == "W" or not self.rna[1:].isdigit()
        ):
            raise ValueError(
                f"RNA invalide : '{self.rna}' (W + 9 chiffres attendus)"
            )

        if self.confiance_jointure is not None and not 0 <= self.confiance_jointure <= 1:
            raise ValueError(
                f"confiance_jointure doit être entre 0 et 1, reçu {self.confiance_jointure}"
            )

        # Auto-détection famille ESS depuis la catégorie juridique
        if self.famille_ess is None and self.categorie_juridique:
            self.famille_ess = CATEGORIE_JURIDIQUE_FAMILLE.get(self.categorie_juridique)

        # Auto-détection tag numérique depuis le NAF
        if self.naf in NAF_NUMERIQUE:
            self.naf_numerique = True
            self.tags_numerique.add(NAF_NUMERIQUE[self.naf])

    def ajouter_source(self, source: Source) -> None:
        """Ajoute une source (dédupliquée automatiquement via set)."""
        self.sources.add(source)

    def ajouter_tag(self, tag: TagNumerique) -> None:
        """Ajoute un tag numérique."""
        self.tags_numerique.add(tag)

    def to_dict(self) -> dict:
        """Sérialise en dict plat (compatible pandas / CSV)."""
        return {
            "siren": self.siren,
            "siret_siege": self.siret_siege,
            "rna": self.rna,
            "tva_intra": self.tva_intra,
            "denomination": self.denomination,
            "nom_usage": self.nom_usage,
            "sigle": self.sigle,
            "categorie_juridique": self.categorie_juridique,
            "categorie_juridique_libelle": self.categorie_juridique_libelle,
            "famille_ess": self.famille_ess.value if self.famille_ess else None,
            "agrement_esus": self.agrement_esus,
            "flag_ess_sirene": self.flag_ess_sirene,
            "flag_ess_liste": self.flag_ess_liste,
            "naf": self.naf,
            "naf_libelle": self.naf_libelle,
            "naf_numerique": self.naf_numerique,
            "tags_numerique": "|".join(sorted(t.value for t in self.tags_numerique)) or None,
            "code_postal": self.code_postal,
            "commune": self.commune,
            "departement": self.departement,
            "region": self.region,
            "tranche_effectifs": self.tranche_effectifs,
            "date_creation": self.date_creation.isoformat() if self.date_creation else None,
            "est_active": self.est_active,
            "site_web": self.site_web,
            "sources": "|".join(sorted(s.value for s in self.sources)),
            "methode_jointure": self.methode_jointure.value,
            "confiance_jointure": self.confiance_jointure,
            "date_integration": self.date_integration.isoformat(),
        }