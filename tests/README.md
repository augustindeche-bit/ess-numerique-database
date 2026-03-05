# ESS Numérique Database

Construction d'un ficher qualifié des structures de  l'**Économie Sociale et Solidaire (ESS)** exerçant une activité **numérique** en France. 
Le projet vise à transformer les datasets bruts (SIRENE, ESS France) en une base scientifiquement crédible, par filtrage progressif, enrichissement multi-sources et validation humaine.

## Architecture technique finale du POC

```
┌──────────────────────────────────────────┐
│  SOURCES : SIRENE + Liste ESS            │
└───────────────────┬──────────────────────┘
                    ▼
┌──────────────────────────────────────────┐
│  PIPELINES                               │
└───────────────────┬──────────────────────┘
                    ▼
┌──────────────────────────────────────────┐
│  LABEL STUDIO                            │
└───────────────────┬──────────────────────┘
                    ▼
┌──────────────────────────────────────────┐
│ Export des données                       │
│  Chiffres clés + fiches orgas qualifiées │
└──────────────────────────────────────────┘
```

## Lancer le pipeline

```bash
python run_pipeline.py               # etapes 1→4 (sans telechargement)
python run_pipeline.py --download     # etapes 0→4 (avec telechargement des sources)
python run_pipeline.py --from 3       # reprendre a partir de l'etape 3
```


### etapes 1→4 (sans telechargement)
```bash
python run_pipeline.py               
```

| Etape | Description | Sortie |
|---|---|---|
| 0 | Telechargement des sources brutes (optionnel) | `data/raw/*` |
| 1 | Filtrage des candidats (4 pipelines) | `01_ess_x_naf.csv`, `02_cate_x_naf.csv`, `03_tiers_lieux.csv`, `04_ess_flag_insee.csv` |
| 2 | Consolidation par SIREN | `x_consolidated.csv` |
| 3 | Enrichissement depuis SIRENE | `x_enriched.csv` |
| 4 | Export pour Label Studio | `x_label_studio.csv` + `label_studio_template.xml` |

Les fichiers de sortie sont **nettoyés avant chaque étape** pour éviter de travailler sur des données obsolètes.

## Tests

```bash
pytest tests/ -v                              # tous les tests
pytest tests/ -v -W error::FutureWarning      # strict (warnings = erreurs)
```

## Structure du projet

```
ess-numerique-database/
├── run_pipeline.py          # Orchestrateur du pipeline complet
├── data/
│   ├── raw/                 # Données brutes (non versionnées)
│   │   ├── StockUniteLegale_utf8.parquet
│   │   ├── entreprisesess.csv
│   │   ├── liste_nationale_esus.xlsx
│   │   ├── bdftl-2023-01-fiche-identite.csv
│   │   └── bdftl-2023-04-activites.csv
│   └── processed/
│       ├── candidates/      # Candidats, consolidation, enrichissement
│       └── export/          # Export Label Studio
├── src/
│   ├── models.py            # Dataclass OrganisationESS, enums, mappings (source de vérité)
│   ├── 00_DataLayer/        # Téléchargement des sources brutes
│   │   ├── 01_sirene.py              → StockUniteLegale_utf8.parquet
│   │   ├── 02_ess_france.py          → entreprisesess.csv
│   │   ├── 03_esus.py                → liste_nationale_esus.xlsx
│   │   └── 04_tiers_lieux.py         → bdftl-2023-*.csv
│   ├── 01_Candidates/       # Filtrage → fichiers candidats
│   │   ├── 01_ess_x_naf.py              → 01_ess_x_naf.csv
│   │   ├── 02_cate_x_naf.py             → 02_cate_x_naf.csv
│   │   ├── 03_tiers_lieux.py            → 03_tiers_lieux.csv
│   │   ├── 04_ess_flag_insee.py          → 04_ess_flag_insee.csv
│   │   └── x_consolated.py              → x_consolidated.csv
│   ├── 02_DataEnrichment/   # Enrichissement post-consolidation
│   │   └── enrich_from_sirene.py         → x_enriched.csv
│   └── 03_Export/           # Export pour annotation
│       └── export_label_studio.py        → x_label_studio.csv + template XML
├── tests/
│   ├── test_x_consolated.py
│   ├── test_ess_flag_insee.py
│   ├── test_enrich_from_sirene.py
│   └── test_export_label_studio.py
└── README.md
```

## 01 - DataLayer : Sources de données

### 1. Stock Unité Légale (INSEE / SIRENE)

- **Jeu de données :** [Base Sirene (SIREN, SIRET) sur data.gouv.fr](https://www.data.gouv.fr/fr/datasets/base-sirene-des-entreprises-et-de-leurs-etablissements-siren-siret)
- **Fichier :** `StockUniteLegale_utf8.parquet` (~30 millions de lignes, actualisé mensuellement)
- **Procédure :** onglet **Ressources / Téléchargements**, rechercher le fichier `.parquet`
> **Attention :** ce fichier est trop volumineux pour Excel. Utiliser Python (pandas/polars) ou DuckDB.

### 2. Entreprises de l'ESS (ESS France)

- **Jeu de données :** [Liste des entreprises de l'ESS](https://www.data.gouv.fr/fr/datasets/liste-des-entreprises-de-less)
- **Fichier :** `liste-des-entreprises-de-less.csv` ou `.parquet` (~1,3 million de lignes)
- **Procédure :** onglet **Ressources**, télécharger le `.csv` ou `.parquet`. Si seul le CSV est visible, data.gouv.fr propose un bouton "Prévisualisation/Analyse" pour accéder à une version optimisée.

### 3. Tiers-Lieux (France Tiers-Lieux)

- **Jeu de données :** [Recensement des Tiers-Lieux en France - 2023](https://www.data.gouv.fr/datasets/recensement-des-tiers-lieux-en-france-2023/)
- **Fichier :** `bdftl-2023-04-activites.csv` (section 04 - Activités)
- **Procédure :** onglet **Ressources**, télécharger le CSV correspondant à la thématique "Activités". Le jeu complet est découpé en plusieurs CSV par thématique (identité, foncier, activités, publics, RH, etc.).
- **Voir aussi :** [Observatoire France Tiers-Lieux - Données](https://observatoire.francetierslieux.fr/donnees/)

## Méthodologie

### Conventions de nommage

#### DataLayer (`00_DataLayer/`)

Chaque script de téléchargement suit la règle **script = source** :

| Script | Source enum | Fichier raw |
|--------|------------|-------------|
| `01_sirene.py` | `sirene` | `StockUniteLegale_utf8.parquet` |
| `02_ess_france.py` | `ess_france` | `entreprisesess.csv` |
| `03_esus.py` | `esus` | `liste_nationale_esus.xlsx` |
| `04_tiers_lieux.py` | `tiers_lieux` | `bdftl-2023-*.csv` |

L'`{id}` du script correspond à la valeur `Source` enum dans `models.py`.

#### Pipelines candidats (`01_Candidates/`)

Chaque pipeline candidat suit la règle **script = output = origine** :

| Composant | Format | Exemple |
|-----------|--------|---------|
| Script | `{NN}_{id}.py` | `01_ess_x_naf.py` |
| Output CSV | `{NN}_{id}.csv` | `01_ess_x_naf.csv` |
| Clé `origines` | `{id}` | `ess_x_naf` |

Le préfixe `{NN}` donne l'ordre d'exécution. L'`{id}` est utilisé partout : clé dans `INPUTS`, valeur dans la colonne `origines`, suffixe des colonnes de conflit (`denomination__{id}`).

| ID | Script | Description |
|----|--------|-------------|
| `ess_x_naf` | `01_ess_x_naf.py` | ESS France × NAF numérique |
| `cate_x_naf` | `02_cate_x_naf.py` | Catégorie juridique ESS × NAF numérique |
| `tiers_lieux` | `03_tiers_lieux.py` | Tiers-lieux à activité numérique |
| `ess_flag_insee` | `04_ess_flag_insee.py` | Flag ESS INSEE × NAF numérique |

### Filtrer entitées candidates [01_Candidates](src/01_Candidates)

Quatre pipelines identifient des structures ESS à activité numérique selon des critères complémentaires :

| Script | Source | Critère | Jointure |
|---|---|---|---|
| `01_ess_x_naf.py` | ESS France + Sirene | SIREN ESS × 10 codes NAF numériques | `siren_direct` |
| `02_cate_x_naf.py` | Sirene + ESUS | Catégorie juridique ESS (ou ESUS) × 10 codes NAF | `siren_direct` |
| `03_tiers_lieux.py` | France Tiers-Lieux + Sirene | Tiers-lieux à activité numérique | `scoring` |
| `04_ess_flag_insee.py` | Sirene | Flag ESS INSEE × 10 codes NAF numériques | `siren_direct` |

#### Consolidation : `x_consolated.py`

Fusionne les 4 fichiers par **SIREN** (clé unique) → `x_consolidated.csv`.

**Colonnes de traçabilité :**

| Colonne | Description | Remplissage |
|---|---|---|
| `sources` | Datasets d'origine (union, ex: `ess_france\|sirene`) | 100 % |
| `origines` | Pipelines contributeurs (ex: `cate_x_naf\|ess_x_naf`) | 100 % |
| `methode_jointure` | Méthode la plus fiable du groupe (`siren_direct` > `scoring`) | 100 % |
| `confiance_jointure` | Score de confiance (1.0 pour `siren_direct`, score réel pour `scoring`) | 100 % |

**Règles de fusion :**
- Attributs concordants entre sources → valeur unique
- Attributs divergents → colonnes de conflit suffixées `<attribut>__<origine>` (voir ci-dessous)
- Sources et tags agrégés (union)
- Enrichissement automatique : `famille_ess`, `naf_numerique`, `tags_numerique`

**Colonnes de conflit (`<attribut>__<origine>`) :**

Quand un même SIREN apparaît dans plusieurs pipelines avec des valeurs **différentes** pour un attribut, la consolidation crée des colonnes suffixées pour tracer chaque version.

Exemple pour le SIREN 333333333 avec 3 dénominations différentes :

| Colonne | Valeur |
|---|---|
| `denomination` | GammaLab (valeur principale = première source) |
| `denomination__ess_x_naf` | GammaLab |
| `denomination__cate_x_naf` | GammaLab-Alt |
| `denomination__tiers_lieux` | GammaFab |

Les attributs concernés par ce mécanisme : `denomination`, `nom_usage`, `naf`, `code_postal`, `commune`, `site_web`, `tranche_effectifs`, `categorie_juridique`.

Ces colonnes sont **vides (NaN)** pour les SIRENs sans conflit (source unique ou valeurs identiques entre sources). Elles ne sont pas enrichies depuis SIRENE — ce sont uniquement des marqueurs de divergence entre pipelines candidats.

#### Règles de calcul des champs dérivés

Ces champs sont calculés automatiquement à partir d'autres champs, lors de la consolidation (`x_consolated.py`) et re-dérivés lors de l'enrichissement (`enrich_from_sirene.py`).

**`naf_numerique`** (booléen) :
- `True` si le code `naf` figure dans le dictionnaire `NAF_NUMERIQUE` (`src/models.py`)
- `False` sinon
- Codes concernés : `62.01Z`, `62.02A`, `62.02B`, `62.03Z`, `63.11Z`, `63.12Z`, `58.21Z`, `58.29A`, `58.29B`, `58.29C`

**`tags_numerique`** (chaîne ou null) :
- Valeur du dictionnaire `NAF_NUMERIQUE` associée au code `naf`
- `None` si le `naf` n'est pas un code numérique

| Code NAF | Tag |
|---|---|
| `62.01Z` | `dev_logiciel` |
| `62.02A`, `62.02B` | `conseil_it` |
| `62.03Z`, `63.11Z` | `hebergement` |
| `63.12Z` | `portail_web` |
| `58.21Z` | `edition_jeux` |
| `58.29A`, `58.29B`, `58.29C` | `edition_logiciel` |

**`famille_ess`** (chaîne ou null) :
- Dérivée du code `categorie_juridique` via le dictionnaire `CATEGORIE_JURIDIQUE_FAMILLE` (`src/models.py`)
- `None` si le code n'est pas reconnu

| Codes catégorie juridique | Famille |
|---|---|
| 9210, 9220, 9221, 9222, 9230, 9240, 9260 | `association` |
| 5458, 5547, 5558, 5560, 6316, 6317, 6318 | `cooperative` |
| 8210, 8250, 8290 | `mutuelle` |
| 9300, 9310, 9900 | `fondation` |

**`est_active`** (booléen ou null) :
- Dérivé de `etatAdministratifUniteLegale` SIRENE
- `"A"` → `True` (active), `"C"` → `False` (cessée)

**`flag_ess_sirene`** (booléen ou null) :
- Dérivé de `economieSocialeSolidaireUniteLegale` SIRENE
- `"O"` → `True`

**`siret_siege`** (chaîne ou null) :
- Concaténation de `siren` (9 chiffres) + `nicSiegeUniteLegale` (5 chiffres) = 14 chiffres
- Validé par regex `^\d{14}$`, ignoré si le NIC est invalide

**`confiance_jointure`** (float 0.0 - 1.0) :
- `1.0` par défaut si `methode_jointure = siren_direct`
- Score réel (0.80 - 1.0) pour `methode_jointure = scoring`
- Maximum des valeurs si plusieurs sources contribuent

### Enrichissement et validation

#### Enrichissement depuis SIRENE : `enrich_from_sirene.py`

Remplit les champs vides du fichier consolidé depuis le parquet SIRENE (~30M lignes).
Chaque champ enrichi reçoit deux colonnes de traçabilité : `<champ>_enrich_source` et `<champ>_enrich_method`.

```
x_consolidated.csv + StockUniteLegale_utf8.parquet → x_enriched.csv
```

**Mapping SIRENE → modèle consolidé :**

| Colonne SIRENE | Champ cible | Transformation |
|---|---|---|
| `activitePrincipaleUniteLegale` | `naf` | copie directe |
| `categorieJuridiqueUniteLegale` | `categorie_juridique` | copie directe |
| `trancheEffectifsUniteLegale` | `tranche_effectifs` | copie directe |
| `denominationUniteLegale` | `denomination` | copie directe |
| `nomUniteLegale` | `nom_usage` | copie directe |
| `sigleUniteLegale` | `sigle` | copie directe |
| `dateCreationUniteLegale` | `date_creation` | copie directe |
| `etatAdministratifUniteLegale` | `est_active` | `"A"` → `True`, `"C"` → `False` |
| `nicSiegeUniteLegale` | `siret_siege` | concaténation `siren + nic` (validé 14 chiffres) |
| `identifiantAssociationUniteLegale` | `rna` | copie directe |
| `economieSocialeSolidaireUniteLegale` | `flag_ess_sirene` | `"O"` → `True` |
| `caractereEmployeurUniteLegale` | `caractere_employeur` | copie directe |
| `categorieEntreprise` | `categorie_entreprise` | copie directe |
| `societeMissionUniteLegale` | `societe_mission` | copie directe |
| `anneeEffectifsUniteLegale` | `annee_effectifs` | copie directe |

**Règle d'enrichissement :** seuls les champs vides/NaN sont remplis ; les valeurs existantes ne sont jamais écrasées.

#### Enrichissement multi-sources (prévu)

Les codes NAF seuls ne suffisent pas à qualifier une activité numérique. Enrichissement prévu par :
- Descriptions d'objets sociaux via l'INPI (open data)
- Vérification d'existence de sites web
- Données complémentaires data.gouv.fr
- Bases sectorielles ciblées (répertoire des fab labs, bases ANCT)
- Web scraping ciblé pour les cas ambigus (notamment les 94.99Z)

### Export pour Label Studio : `export_label_studio.py`

Prépare un CSV simplifié (colonnes métier uniquement) et un template XML d'annotation pour Label Studio.

```
x_enriched.csv → x_label_studio.csv + label_studio_template.xml
```

**Colonnes supprimées :**
- `*_enrich_source` / `*_enrich_method` — traçabilité technique
- `*__<origine>` — colonnes de conflit entre pipelines

**Template d'annotation :**
- Affichage structuré des informations de la structure
- Choix de classification : **OUI** / **NON** / **A ENRICHIR**
- Champ texte pour la catégorie d'activité numérique
- Champ texte pour commentaire / source de la décision

### Validation humaine via Label Studio (hors repo)
Validation structurée et auditable via [Label Studio](https://labelstud.io/) (outil d'annotation open source) déployé sur serveur dédié (Docker/Debian).

**Workflow multi-annotateurs :**
```
Annotation initiale
        │
        ▼
  Revue experte ESS
        │
        ├── Accord → Validation confirmée
        └── Désaccord →  (arbitrage)
```

**Critères d'annotation :**
- Statut ESS numérique confirmé (OUI / NON / A ENRICHIR)
- Catégorie d'activité numérique (selon la typologie définie en phase A)
- Source de la décision (URL, connaissance du réseau...)
- Informations manquantes le cas échéant
- Évaluation de la fiabilité des données enrichies

Approche méthodologique : annotation continue et shufflée (pas de lots figés), permettant un suivi en temps réel de la qualité et le calcul de l'accord inter-annotateurs.

## Licence

MIT -- voir [LICENSE](LICENSE).