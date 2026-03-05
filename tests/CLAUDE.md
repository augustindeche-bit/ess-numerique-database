# CLAUDE.md - Guidelines de developpement

## Projet

Base de donnees qualifiee des structures ESS a activite numerique en France.
Pipeline : Sources brutes → Candidats → Enrichissement → Validation humaine → Export.

## Structure du code

```
notebooks/                           # Exploration, prototypage, validation des methodes
src/
  models.py                          # Dataclass OrganisationESS, enums, mappings (source de verite)
  resolve_siren_by_name.py           # Resolution SIREN par nom (exact, alternatif, fuzzy)
  00_DataLayer/                      # Download et integration des sources brutes
    01_sirene.py                     # Telechargement SIRENE → StockUniteLegale_utf8.parquet
    02_ess_france.py                 # Telechargement ESS France → entreprisesess.csv
    03_esus.py                       # Telechargement ESUS → liste_nationale_esus.xlsx
    04_tiers_lieux.py                # Telechargement Tiers-Lieux → bdftl-2023-*.csv
  01_Candidates/                     # Pipelines de filtrage → fichiers candidats
    01_ess_x_naf.py                  # ESS France × NAF → 01_ess_x_naf.csv
    02_cate_x_naf.py                 # Cate juridique × NAF → 02_cate_x_naf.csv
    03_tiers_lieux.py                # Tiers-lieux numeriques → 03_tiers_lieux.csv
    04_ess_flag_insee.py             # Flag ESS INSEE × NAF → 04_ess_flag_insee.csv
    x_consolated.py                  # Consolidation par SIREN → x_consolidated.csv
  02_DataEnrichment/                 # Enrichissement post-consolidation
    enrich_from_sirene.py            # Enrichissement depuis SIRENE → x_enriched.csv
  03_Export/                         # Export pour validation humaine
    export_label_studio.py           # Export CSV + template XML Label Studio
tests/
  test_x_consolated.py              # Tests consolidation
  test_ess_flag_insee.py             # Tests pipeline ESS flag INSEE
  test_enrich_from_sirene.py         # Tests enrichissement SIRENE
  test_export_label_studio.py        # Tests export
```

## Documentation du projet

| Fichier | Role | Contenu |
|---------|------|---------|
| `USER_STORY_PHASE_B.md` | **Objectifs et attentes** | User stories, criteres d'acceptation, statut de chaque livrable. Le developpement doit etre en accord avec ces US. |
| `README.md` | **Regles fonctionnelles et metier** | Architecture, methodologie, regles de calcul des champs derives, mapping SIRENE, instructions pour lancer le pipeline et les tests. |
| `CLAUDE.md` | **Conventions de developpement** | Structure du code, patterns techniques, conventions de test, workflow de dev. |

Avant de developper une fonctionnalite, verifier les US concernees dans `USER_STORY_PHASE_x.md`.
Pour les regles metier detaillees (mappings, champs derives, enrichissement), se referer au `README.md`.

## Workflow de developpement

### Notebooks → Scripts → Tests
1. **Notebooks** (`notebooks/`) : explorer les donnees, prototyper les methodes, valider les analyses
2. **Scripts** (`src/`) : implementer la version production du code valide en notebook
3. **Tests** (`tests/`) : ecrire les tests unitaires pour le script
4. **Documentation** : mettre a jour `USER_STORY_PHASE_B.md` (statut US) et le `README.md` (regles metier) si necessaire

Les notebooks ne font pas partie du pipeline de production. Ils servent a tester et valider avant implementation dans `src/`.

## Conventions de code

### Imports et paths
- `ROOT = Path(__file__).resolve().parents[2]` pour remonter a la racine projet
- `sys.path.insert(0, str(ROOT))` puis `from src.models import ...`
- Toujours utiliser des objets `Path`, jamais des strings pour les chemins
- Operateur `/` pour joindre les paths : `ROOT / "data" / "raw" / "fichier.csv"`

### Pandas et types
- Toujours `dtype={"siren": str}` lors du `read_csv`
- SIREN : string 9 chiffres, zero-padde, valide par regex `^\d{9}$`
- SIRET : string 14 chiffres, valide par regex `^\d{14}$`
- Nettoyage float SIREN : `.astype(str).str.strip().str.split(".").str[0].str.zfill(9)`
- Colonnes texte nullable : utiliser dtype `object` pour eviter les FutureWarning pandas
- Fichiers parquet volumineux (SIRENE ~30M lignes) : toujours lire par row-groups, jamais en une seule fois
  ```python
  pf = pq.ParquetFile(path)
  for batch in pf.iter_batches(batch_size=pf.metadata.row_group(0).num_rows):
      chunk = batch.to_pandas()
      # filtrer/traiter le chunk
  ```

### Tracabilite des enrichissements
Chaque champ enrichi doit avoir deux colonnes associees :
- `<champ>_enrich_source` : identifiant de la source (ex: `"sirene"`)
- `<champ>_enrich_method` : methode utilisee (ex: `"siren_direct"`)

### Pattern des scripts
- Config et constantes en haut du fichier
- Fonctions utilitaires au milieu
- `main()` pour l'orchestration avec print de progression
- `if __name__ == "__main__": main()` en bas
- `main()` accepte des paths en parametres pour permettre les tests

### Nommage des scripts (regle globale)
Convention : `{NN}_{id}.py` dans chaque dossier. L'`{id}` est reutilise partout (outputs, cles, colonnes).

**DataLayer** (`00_DataLayer/`) : `{NN}_{source_id}.py`
- L'`{id}` correspond a la valeur `Source` enum dans `models.py` (ex: `sirene`, `ess_france`)

**Candidates** (`01_Candidates/`) : `{NN}_{id}.py` → `{NN}_{id}.csv` → cle origines `{id}`
- L'`{id}` est utilise comme cle dans `INPUTS`, valeur dans la colonne `origines`, et suffixe des colonnes de conflit (`denomination__{id}`)

Lors de l'ajout d'un nouveau script, respecter cette convention pour garantir la coherence entre scripts, fichiers et donnees de sortie.

### Modele de donnees
- Source de verite : `src/models.py`
- Dictionnaires de mapping : `NAF_NUMERIQUE`, `CATEGORIE_JURIDIQUE_FAMILLE`, `GROUP_NAF`, `GROUP_CATE`
- Enums : `Source`, `MethodeJointure`, `FamilleESS`, `TagNumerique`
- Ne pas dupliquer ces mappings dans d'autres fichiers, toujours importer depuis `models.py`

## Conventions de test

### Import des modules (dossiers commencant par un chiffre)
```python
import importlib.util
_spec = importlib.util.spec_from_file_location(
    "nom_module",
    ROOT / "src" / "02_DataEnrichment" / "nom_module.py",
)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
```

### Structure des tests
- Pytest avec classes par fonction testee : `TestNomFonction`
- Fixtures `@pytest.fixture` pour les fichiers temporaires (`tmp_path`)
- Helpers : `_write_csv()`, `_make_group()`, `_make_consolidated()`
- Donnees simulees couvrant tous les cas limites (concordant, divergent, NaN, multi-source)
- Assertions sur valeurs exactes, presence de colonnes, `pd.isna()` pour les nulls

### Lancer les tests
```bash
pytest tests/ -v                              # tous les tests
pytest tests/test_enrich_from_sirene.py -v    # un fichier specifique
pytest tests/ -v -W error::FutureWarning      # strict (warnings = erreurs)
```

## Regles metier (rappels cles)

Les regles metier completes sont dans le `README.md`. Rappels critiques pour le dev :
- Ne jamais ecraser une valeur existante : enrichir uniquement les champs vides/NaN
- Parquet SIRENE : lire par row-groups pour gerer les ~30M lignes
- Deduplication SIRENE : garder la periode la plus recente (`dateDebut` desc)
- Champs derives (`famille_ess`, `naf_numerique`, `tags_numerique`) : re-deriver apres enrichissement de `naf` ou `categorie_juridique`

## Git

- Branche principale : `main`
- Ne pas versionner `data/raw/` ni `data/processed/` (dans `.gitignore`)
- Commits en anglais ou francais, preferer des messages concis
