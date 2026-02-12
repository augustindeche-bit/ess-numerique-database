Travail sur 2 BDD : 
- entreprisess.csv tirée de ESS France avec 1 300 000 lignes --> base ESS
- stockunite légale en .parquet avec TOUTES les structures déclarées à la République Fraçaise (quasi 30 millions de lignes) --> base gouv

Dans la base ESS on a siren le nom le lieu la famille juridique
Dans la base gouv ce qui nous intéresse c'est le code NAF surtout et le siren

Dans explration des data je regarde ce que cela contient (+ apprentissage sur le .parquet car 1ère utilisation)


Dans Filtrage data je m'occupe de faire les 2 filtres : 
-garder que certaines colonnes de data gouv .parquet sinon mon ordi explose et filtrer sur les codes naf concernés ET si ess ou non
- comparer ce doc et enrteprisesess par le siren pour vérifier les conditions ESS légales. il semblerait en effte que la colonne ESSunitélégale est gloablement pas fiable. 

Dans sortie je reprends le doc créé et j'en fait un csv

Dans gestion data 94.99Z j'ai commencé à regarder le nombre de structures dans ce code naf fourre-tout que nous devrons trier, à voir le nombre (aujourd'hui bien trop -->520 000 structures)