# User Stories — Phase B

## Pipeline hybride de resolution des jointures

Le pipeline utilise plusieurs strategies de resolution. Chaque candidat porte la trace de sa methode de jointure (`methode_jointure`) et de son score de confiance (`confiance_jointure`).

### Valeurs de `MethodeJointure` (enum dans `models.py`)

| Valeur | Description | Confiance | Statut |
|--------|-------------|-----------|--------|
| `SIREN_DIRECT` | Jointure directe par SIREN | 1.0 | [x] `01_ess_x_naf.py`, `02_cate_x_naf.py`, `04_ess_flag_insee.py` |
| `SCORING` | Resolution par nom (exact, alternatif, fuzzy) | 0.8–1.0 | [x] `resolve_siren_by_name.py` (3 passes), utilise par `03_tiers_lieux.py` |
| `VIA_RNA` | Resolution via identifiant RNA | — | [ ] Non implemente |
| `API_SIRENE` | Resolution via API SIRENE | — | [ ] Non implemente |
| `SCRAPING` | Resolution par scraping web | — | [ ] Non implemente |
| `MANUEL` | Resolution manuelle | variable | [ ] Non implemente (prevu : notebook NGI, annuaire-entreprises.data.gouv.fr) |

### Detail des 3 passes de `resolve_siren_by_name.py` (methode `SCORING`)

| Passe | Methode | Confiance |
|-------|---------|-----------|
| 1 | Match exact par nom normalise | 1.0 |
| 2 | Match exact sur nom alternatif | 0.9 |
| 3 | Match fuzzy (token_sort_ratio >= 80) | score/100 |

---

## US B.0 : Mise a disposition des documents de reference
**Statut** : A DOCUMENTER

- [ ] Dossier partage accessible a l'equipe (GitHub ou drive)
- [ ] Documents d'Augustin : sources de donnees repertoriees, criteres de filtrage, nomenclatures
- [ ] Lien vers le dossier dans le README ou la documentation projet

---

## US F.0 : Modele de donnees
**Fichier** : [models.py](src/models.py)
**Statut** : DONE

Dataclass `OrganisationESS` avec :
- Enums : `Source`, `MethodeJointure`, `FamilleESS`, `TagNumerique`
- Mappings : `NAF_NUMERIQUE` (10 codes NAF numeriques), `CATEGORIE_JURIDIQUE_FAMILLE` (20 codes juridiques vers 4 familles)
- Validation auto des SIREN/SIRET/RNA
- Derivation auto de `famille_ess`, `naf_numerique`, `tags_numerique`
- Note : `FamilleESS.ENTREPRISE_ESS` existe dans l'enum mais aucun code juridique n'y mappe dans `CATEGORIE_JURIDIQUE_FAMILLE` (attribution manuelle ou par source, pas par categorie juridique)

---

## US F.1 : Data Layer (telechargement des sources brutes)
**Dossier** : [00_DataLayer](src/00_DataLayer)
**Statut** : DONE

4 scripts de telechargement depuis data.gouv.fr :
- [x] `01_sirene.py` — SIRENE (parquet, ~30M lignes)
- [x] `02_ess_france.py` — ESS France (~1.3M lignes)
- [x] `03_esus.py` — Agrements ESUS (xlsx)
- [x] `04_tiers_lieux.py` — France Tiers-Lieux (identites + activites)

---

## US F.2 : Pipelines candidats
**Dossier** : [01_Candidates](src/01_Candidates)
**Statut** : DONE

4 pipelines independants de filtrage :
- [x] `01_ess_x_naf.py` — SIREN ESS France x NAF numeriques (~1049 candidats)
- [x] `02_cate_x_naf.py` — Categories juridiques ESS (+ ESUS) x NAF numeriques (~863 candidats)
- [x] `03_tiers_lieux.py` — Tiers-lieux avec activites numeriques (~16 candidats)
- [x] `04_ess_flag_insee.py` — Flag ESS INSEE x NAF numeriques

Utilise [resolve_siren_by_name.py](src/resolve_siren_by_name.py) pour la resolution SIREN par nom (matching exact, alternatif, fuzzy).

---

## US A.3 : Consolidation par SIREN
**Fichier** : [x_consolated.py](src/01_Candidates/x_consolated.py)
**Statut** : DONE

- [x] Fusion des 4 fichiers candidats par SIREN unique (~1020 SIREN)
- [x] Detection de conflits (colonnes suffixees `__origin`)
- [x] Agregation des sources et origines
- [x] Selection de la meilleure methode de jointure
- [x] Derivation des champs `famille_ess`, `naf_numerique`, `tags_numerique`
- [x] Tests unitaires : [test_x_consolated.py](tests/test_x_consolated.py)

---

## US B.0.1 : Enrichissement NGI (Next Generation Internet)
**Notebook** : [Enrich_NGI_data.ipynb](notebooks/Filtrage%20progressif/Enrich_NGI_data.ipynb)
**Statut** : PARTIEL (semi-manuel)

- [x] Chargement du CSV NGI (46 projets, 38 impliquant la France)
- [x] Filtrage des projets France
- [x] Extraction des domaines depuis les sites web des projets
- [ ] Resolution manuelle des SIREN via annuaire-entreprises.data.gouv.fr (etape 5 du pipeline)
- [ ] Croisement avec la base ESS pour tagger les structures porteuses de projets NGI

---

## US B.1 : Refactoring resolve_siren_by_name
**Fichier** : [resolve_siren_by_name.py](src/resolve_siren_by_name.py)
**Statut** : A FAIRE

Le module fonctionne (utilise par 03_tiers_lieux.py) mais doit etre refactorise :
- [ ] Convertir en classe
- [ ] Permettre de choisir les passes de matching (exact, alternatif, fuzzy)
- [ ] Choisir dynamiquement les colonnes sources du DataFrame
- [ ] Gerer les matches n-n (non par defaut, avec indication)
- [ ] Tests unitaires

---

## US B.2 : Enrichissement depuis SIRENE
**Fichier** : [enrich_from_sirene.py](src/02_DataEnrichment/enrich_from_sirene.py)
**Statut** : DONE

- [x] Enrichissement des champs vides depuis le parquet SIRENE (14 champs)
- [x] Ne jamais ecraser une valeur existante
- [x] Tracabilite : `<champ>_enrich_source` + `<champ>_enrich_method` pour chaque champ enrichi
- [x] Deduplication SIRENE (periode la plus recente)
- [x] Lecture par row-groups pour gerer les ~30M lignes
- [x] Transforms : `est_active` (A/C → bool), `flag_ess_sirene` (O → bool)
- [x] Calcul `siret_siege` depuis SIREN + NIC
- [x] Re-derivation `famille_ess`, `naf_numerique`, `tags_numerique` apres enrichissement
- [x] Tests unitaires : [test_enrich_from_sirene.py](tests/test_enrich_from_sirene.py)

---

## US B.3 : Export Label Studio
**Fichier** : [export_label_studio.py](src/03_Export/export_label_studio.py)
**Statut** : DONE

- [x] Filtrage des colonnes (suppression des colonnes techniques `*_enrich_*`, `*__origin*`)
- [x] Calcul d'un score d'enrichissement (0-1, % de 12 champs cles remplis)
- [x] Generation du template XML Label Studio (panneaux : Identite, Classification, Activite, Localisation, Qualite)
- [x] Export CSV propre (27 colonnes metier) + template XML
- [x] Tests unitaires : [test_export_label_studio.py](tests/test_export_label_studio.py)

---

## US C.0 : Orchestrateur pipeline
**Fichier** : [run_pipeline.py](run_pipeline.py)
**Statut** : DONE

- [x] Enchainement de toutes les etapes (download optionnel → candidats → consolidation → enrichissement → export)
- [x] Execution selective des etapes

---

## US C1.0 : Pipeline candidat NGI (Next Generation Internet)
**Statut** : A FAIRE (phase C1)

- [ ] Script `src/01_Candidates/NGI_numerique.py` — pipeline candidat depuis csvExportNGI.csv
- [ ] Resolution SIREN par nom (via `resolve_siren_by_name.py`)
- [ ] Integration dans la consolidation (`x_consolated.py`)
- [ ] Tests unitaires

---

## Sources d'enrichissement futures (PLANIFIE)

Sources enumerees dans `models.py` (`Source` enum) sans pipeline d'enrichissement implemente :

| Source | Description | Donnees attendues | Dans enum `Source` |
|--------|-------------|-------------------|--------------------|
| SCOP/SCIC | Cooperatives | SIREN, statut cooperatif | oui |
| FFDN | Federation des FAI associatifs | Identifiants, activites reseau | oui |
| MEDNUM | Mediation numerique | Lieux, activites numeriques | oui |
| OpenDataFrance | Collectivites open data | Identifiants, projets | oui |
| Label INR | Institut Numerique Responsable | Labels, certifications | oui |
| Label BCORP | Certification B Corp | Labels, scores | oui |
| Label LUCIE | Label RSE | Labels, certifications | oui |
| SOGA | State of Green Apps | Projets, evaluations | oui |
| AVISE | Portail ESS | Annuaire, fiches structures | oui |
| INPI | Descriptions objet social | Texte libre, classification | **non — a ajouter** |

Chaque source future reutilisera le pipeline hybride de jointure via `resolve_siren_by_name.py` (apres refactoring US B.1).