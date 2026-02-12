
## 🏗️ Récupération des Données SIRENE & ESS
### 1. Stock Unité Légale (Gouvernement / INSEE)

* **Lien direct du jeu de données :** [Base Sirene (SIREN, SIRET) sur data.gouv.fr](https://www.data.gouv.fr/fr/datasets/base-sirene-des-entreprises-et-de-leurs-etablissements-siren-siret)
1. Rendez-vous sur la page ci-dessus.
2. Allez dans l'onglet **Ressources / Téléchargements**.
3. Recherchez le fichier nommé `StockUniteLegale_utf8.parquet` (ou une variante datée, en vrai ils actualisent tous les mois).


### 2. Entreprises de l'ESS (ESS France)


* **Lien du jeu de données :** [Liste des entreprises de l'ESS](https://www.data.gouv.fr/fr/datasets/liste-des-entreprises-de-less)
* **Format :**  disponible en `.csv`, le format `.parquet` est désormais généré automatiquement par la plateforme data.gouv.fr pour les fichiers volumineux.
Le csv focntionne ici parce qu'il fait un peu plus d'un million de lignes donc ca va. Ce qui n'est pas possbile pour les 30 millions de la base unité légale (WARNING : ce n'est donc pas affichable entièrement sur excel car il y a un peu trop de lignes)
* **Procédure :**
1. Dans l'onglet **Ressources**, cherchez le fichier `liste-des-entreprises-de-less.parquet`, ou .csv
2. Si seul le CSV est visible, data.gouv.fr propose souvent un bouton "Prévisualisation/Analyse" qui permet de générer ou d'accéder à une version optimisée via leur API de données.