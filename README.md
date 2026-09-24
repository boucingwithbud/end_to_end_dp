Oui — et c'est là que la distinction devient intéressante.

AUTO CDC de Databricks SDP/Lakeflow Declarative Pipelines attend justement une source de changements (CDC), contrairement au snapshot dbt classique.
AUTO CDC attend des événements

Typiquement :

customer_id | name  | city   | operation | sequence
1           | Paul  | Paris  | INSERT    | 1
2           | Alice | Lyon   | INSERT    | 2
1           | Paul  | Rennes | UPDATE    | 3
2           | Alice | Lyon   | DELETE    | 4

Avec une colonne permettant d'ordonner les changements.

Dans SDP, tu déclares quelque chose du genre :

dp.create_auto_cdc_flow(
    target="customers_silver",
    source="customers_bronze",
    keys=["customer_id"],
    sequence_by="sequence",
    stored_as_scd_type=2
)

Le moteur comprend alors :

INSERT → nouvelle version
UPDATE → ferme ancienne version + nouvelle version
DELETE → fermeture/suppression selon configuration

Donc compare bien les trois
Technologie	Ce qu'elle reçoit	Qui détecte le changement ?
dbt snapshot	état courant	dbt
SDP AUTO CDC	événements CDC	la source fournit le changement
SCD2 manuel	selon ton choix	toi

Visuellement :

dbt snapshot

Source état courant
       │
       ▼
   comparaison
       │
       ▼
      SCD2

contre :

SDP AUTO CDC

Source CDC
   │
   ├── INSERT
   ├── UPDATE
   └── DELETE
        │
        ▼
      AUTO CDC
        │
        ▼
       SCD2

Et c'est exactement pourquoi ton projet Databricks était différent

Dans ton pipeline SDP, si ton Bronze reçoit des changements CDC, create_auto_cdc_flow() est adapté.

Si tu avais simplement :

customers_bronze
────────────────────
customer_id | name | city | updated_at

avec uniquement l'état courant, alors AUTO CDC n'aurait pas les événements INSERT/UPDATE/DELETE nécessaires. Il faudrait d'abord produire ces changements, par exemple avec CDF, une source CDC, ou une autre logique de détection.

Donc oui : AUTO CDC est beaucoup plus proche d'une vraie architecture CDC que dbt snapshot.

sur les plateformes d'ingestion(databricks et airbyte) on peut indiquer quel type de source on veut cdc ou snapshot 

Oui, mais pas exactement sous la forme “je choisis CDC ou snapshot” partout. Ça dépend de la plateforme et surtout de ce que la source est capable de fournir.
Airbyte

Dans Airbyte, tu peux avoir différents modes de réplication, notamment :

    Full Refresh → Airbyte récupère l'état complet de la source

    Incremental → Airbyte récupère les nouvelles/modifiées selon un curseur

    CDC → pour les sources qui supportent réellement la Change Data Capture

Airbyte distingue donc bien ces modes au niveau du sync/stream.

Par exemple :

PostgreSQL
    │
    ├── Full Refresh
    │
    ├── Incremental
    │
    └── CDC
          │
          ▼
      Databricks

Mais CDC n'est pas disponible pour n'importe quelle source : il faut que le connecteur/source et généralement la base sous-jacente permettent ce mécanisme.
Databricks / Lakeflow

Databricks est encore plus explicite aujourd'hui : il distingue :

source CDC
    ↓
AUTO CDC

et

source snapshots
    ↓
AUTO CDC FROM SNAPSHOT

Databricks documente explicitement ces deux APIs. AUTO CDC traite un flux de changements, tandis que AUTO CDC FROM SNAPSHOT compare des snapshots successifs pour déterminer les changements.

Donc :

             SOURCE
                │
       ┌────────┴────────┐
       │                 │
      CDC            SNAPSHOT
       │                 │
       ▼                 ▼
 AUTO CDC       AUTO CDC FROM SNAPSHOT
       │                 │
       └────────┬────────┘
                ▼
              SCD2

Et c'est important pour ton raisonnement

Tu peux donc avoir cette architecture :

              Airbyte
                 │
       ┌─────────┴─────────┐
       │                   │
      CDC             Full Refresh
       │                   │
       ▼                   ▼
    Databricks          snapshots
       │                   │
   AUTO CDC       AUTO CDC FROM SNAPSHOT
       │                   │
       └─────────┬─────────┘
                 ▼
                SCD2

Donc oui, ton intuition précédente était bonne : dans une architecture moderne, les plateformes d'ingestion peuvent travailler soit avec un flux CDC, soit avec des états/snapshots.

La différence est que dbt Snapshot et Databricks AUTO CDC FROM SNAPSHOT sont deux implémentations différentes de l'idée « je n'ai pas de CDC, je compare des états successifs ». Databricks propose d'ailleurs explicitement cette deuxième API.

Et si tu as un vrai flux CDC avec INSERT/UPDATE/DELETE, AUTO CDC est directement conçu pour ça, avec SEQUENCE BY pour ordonner les événements et APPLY AS DELETE WHEN pour identifier les suppressions.

Fichier markdown(8).md collé
Fichier
boucingwithbud/end_to_end_dp pour ce repo je veux que tu modifies le readme en précisant que le SCD type 2 a été implémenté à partir de cdc et non de snapshot voici le readme actuel

J’ai modifié le README pour préciser explicitement que :

    le SCD Type 2 est implémenté à partir d’événements CDC

    le pipeline utilise Auto CDC

    il ne repose pas sur une comparaison de snapshots

    les opérations INSERT, UPDATE et DELETE sont traitées comme événements CDC

    la section « Ce que démontre le projet » reflète également cette approche

Le README complet est enregistré dans ta Library sous /README.md.

Tu peux aussi récupérer le fichier directement ici :
README.md.
README.mdDocument


README.md
End-to-End Retail Data Platform

Pipeline de données retail construit avec Databricks et Spark Declarative Pipelines (SDP).

Le projet met en œuvre un traitement incrémental basé sur Spark Structured Streaming, depuis l'ingestion de fichiers JSON/CSV jusqu'à la construction d'un modèle analytique en étoile.

L'ensemble du projet est versionné et déployable avec Databricks Asset Bundles (DAB), avec une chaîne CI/CD GitHub Actions permettant de valider et déployer les ressources Databricks.
Architecture

                         SOURCES
                            │
             ┌──────────────┴──────────────┐
             │                             │
       Customer JSON                 Orders CSV / JSON
             │                             │
             └──────────────┬──────────────┘
                            │
                            ▼
                     ┌─────────────┐
                     │ Auto Loader │
                     │  Streaming  │
                     └──────┬──────┘
                            │
                            ▼
                    ┌────────────────┐
                    │     BRONZE     │
                    │                │
                    │ customers      │
                    │ orders         │
                    └───────┬────────┘
                            │
                            ▼
                    ┌────────────────┐
                    │     SILVER     │
                    │                │
                    │ Quality        │
                    │ Deduplication  │
                    │ Normalisation  │
                    │ CDC / SCD2     │
                    └───────┬────────┘
                            │
                            ▼
                    ┌────────────────┐
                    │      GOLD      │
                    │                │
                    │ dim_customer   │
                    │ dim_product    │
                    │ dim_date       │
                    │ dim_channel    │
                    │ fact_orders    │
                    └───────┬────────┘
                            │
                            ▼
                    BI / Analytics

1. Ingestion — Bronze

Les données sont déposées dans des volumes Databricks sous forme de fichiers JSON et CSV. Les événements de changement client sont ingérés avec les informations nécessaires au traitement CDC.

Le pipeline utilise Auto Loader avec cloudFiles pour détecter et traiter progressivement les nouveaux fichiers.
Customers

Les changements clients sont ingérés depuis des fichiers JSON :

spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .load(...)

Les données sont conservées dans :

sdp_data_pipeline.bronze.customers_bronze

La table Bronze est configurée comme une table append-only afin de conserver les événements entrants.
Orders

Les commandes provenant de plusieurs filiales sont ingérées vers une même table Bronze.

Bright Home Orders
        │
        ├── CSV
        │
        ▼
┌──────────────────┐
│                  │
│                  │
│ orders_bronze    │
│                  │
│                  │
└──────────────────┘
        ▲
        │
Lumina Sports
        │
Northstar Outfitters

Les différentes sources utilisent des formats différents :

    Bright Home : CSV

    Lumina Sports : CSV

    Northstar Outfitters : JSON

Les trois flux alimentent :

sdp_data_pipeline.bronze.orders_bronze

Cette approche permet de centraliser les événements de commandes provenant de plusieurs systèmes sources.
2. Transformation — Silver

La couche Silver constitue la couche de préparation et de qualité des données. La gestion du cycle de vie des clients repose sur des événements CDC et non sur un mécanisme de snapshot.

Elle réalise notamment :

    validation des données ;

    filtrage des enregistrements invalides ;

    déduplication ;

    gestion des événements en retard ;

    normalisation des chaînes ;

    conversion des types ;

    pseudonymisation de certaines données clients ;

    gestion des changements clients avec CDC.

Customers

Les données clients sont consommées en streaming depuis Bronze.

Avant d'être appliquées à la table Silver, plusieurs règles sont exécutées.
Data quality

Exemple :

@dp.expect_or_drop("valid_id", "customer_id IS NOT NULL")

Les enregistrements ne contenant pas de customer_id sont rejetés.

Une autre règle vérifie que la ville est renseignée :

customers_rule = {
    "valid_city": "city IS NOT NULL"
}

Les données sont ensuite nettoyées et dédupliquées.
Gestion des données en retard

Le pipeline utilise un watermark :

.withWatermark("timestamp", "1 hours")

Cela permet de définir une fenêtre de tolérance pour les événements arrivant en retard tout en permettant à Spark de gérer l'état du streaming.

La déduplication est ensuite réalisée avant l'application des changements.
Pseudonymisation

Certaines données personnelles sont transformées avant leur exposition dans la couche Silver.

Par exemple :

.withColumn("first_name", F.sha2(F.col("first_name"), 256))
.withColumn("last_name", F.sha2(F.col("last_name"), 256))
.withColumn("email", F.sha2(F.col("email"), 256))

Les valeurs originales ne sont donc pas directement propagées vers les couches analytiques.
3. Change Data Capture et SCD Type 2

Le SCD Type 2 du projet est implémenté à partir de Change Data Capture (CDC). Le pipeline ne repose donc pas sur une comparaison périodique de snapshots de la table source : les événements de changement sont fournis en entrée et appliqués dans l'ordre à la table Silver.

Les changements des clients sont gérés avec Auto CDC à partir d'événements CDC. Les opérations de type insertion, mise à jour et suppression sont interprétées à partir des événements entrants, puis appliquées à la table Silver afin de maintenir l'historique SCD Type 2.

Bronze
   │
   ▼
customers_vw
   │
   ▼
Auto CDC
   │
   ▼
customers_silver

Le pipeline utilise :

dp.create_auto_cdc_flow(
    target="sdp_data_pipeline.silver.customers_silver",
    source="customers_vw",
    keys=["customer_id"],
    sequence_by="timestamp",
    stored_as_scd_type=2
)

La clé métier est :

customer_id

et les événements sont ordonnés selon :

timestamp

Les changements sont conservés en SCD Type 2, ce qui permet de préserver l'historique des versions d'un client.
4. Orders — Silver

Les commandes sont également traitées en streaming depuis Bronze.

Les règles de qualité vérifient notamment :

order_id IS NOT NULL
qty > 0
unit_price > 0
discount_pct > 0
total_amount > 0

Les données sont ensuite normalisées :

String
  ↓
trim
  ↓
normalisation

Timestamp
  ↓
cast timestamp

Quantity
  ↓
integer

Price / Amount
  ↓
decimal(12,2)

Order date
  ↓
date

La déduplication des commandes est réalisée à partir de :

order_id

La table obtenue est :

sdp_data_pipeline.silver.orders_silver

5. Gold — Modèle analytique

La couche Gold transforme les données Silver en un modèle dimensionnel de type Kimball.

Le modèle est organisé autour d'une table de faits et de plusieurs dimensions.

                    dim_customer
                         │
                         │
                         ▼
dim_date ───────► fact_orders ◄────── dim_product
                         │
                         │
                         ▼
                    dim_channel

Fact table
fact_orders

La table de faits contient les événements de commande.

Elle contient notamment :

order_id
customer_id
product_key
date_key
channel_key
order_timestamp
qty
unit_price
discount_pct
coupon_code
total_amount

Les clés des dimensions sont récupérées lors de la construction de la table de faits.
Dimensions
dim_customer

Dimension client construite à partir des données de commandes.

customer_id
region
country
city

dim_product

Dimension produit basée sur :

sku
category

avec une clé surrogate :

product_key

dim_date

Dimension calendrier contenant notamment :

date_key
date
year
quarter
month
month_name
day
day_of_week
day_name
week_of_year
is_weekend

Cette dimension facilite les analyses temporelles.
dim_channel

Dimension représentant les différents canaux de vente :

channel_key
channel

6. Pourquoi Spark Declarative Pipelines ?

Le pipeline utilise Spark Declarative Pipelines pour déclarer les différentes tables, vues et flows du pipeline.

La logique est organisée autour des dépendances entre les datasets :

Bronze
  │
  ▼
Silver
  │
  ├── customers
  │      ↓
  │    Auto CDC
  │      ↓
  │    SCD2
  │
  └── orders
         │
         ▼
       Gold
         │
         ├── dimensions
         └── fact_orders

Cette approche permet de décrire la logique du pipeline sans avoir à gérer manuellement l'ordre d'exécution de chaque étape.
7. Databricks Asset Bundles

Les ressources Databricks sont déployées avec Databricks Asset Bundles (DAB).

Le bundle permet de traiter la configuration Databricks comme du code versionné.

Git repository
       │
       ▼
Databricks Asset Bundle
       │
       ├── configuration
       ├── pipeline
       ├── jobs / resources
       └── environments

Les ressources du projet peuvent ainsi être déployées de manière reproductible plutôt que configurées manuellement dans l'interface Databricks.
Environnements

Le projet distingue les environnements de déploiement.

                Git
                 │
                 ▼
          Databricks Bundle
             /       \
            /         \
         DEV           PROD

Le target dev est utilisé pour le développement et la validation du projet.

Les configurations spécifiques aux environnements sont définies dans le bundle plutôt que dupliquées dans le code du pipeline.
8. Infrastructure as Code

La configuration des ressources Databricks est versionnée dans Git.

Cela permet de rapprocher le projet d'une approche Infrastructure as Code (IaC) :

Code
 +
Configuration
 +
Databricks resources
        │
        ▼
       Git
        │
        ▼
Reproductible / versionné

L'objectif est de pouvoir recréer ou modifier l'environnement Databricks à partir de fichiers versionnés plutôt que de dépendre exclusivement de configurations manuelles dans le workspace.

Databricks Asset Bundles joue ici le rôle de couche de déploiement et de gestion de configuration des ressources Databricks.
9. CI/CD avec GitHub Actions

Le repository utilise GitHub Actions pour automatiser la validation et le déploiement.

Le workflow suit une logique de type :

Developer
    │
    ▼
Git push / Pull Request
    │
    ▼
GitHub Actions
    │
    ├── Validation
    ├── Tests
    └── databricks bundle validate
             │
             ▼
        Deployment
             │
             ▼
        Databricks

Continuous Integration

La CI vérifie que les modifications proposées peuvent être intégrées au projet.

Exemples :

databricks bundle validate -t dev

ainsi que les tests et vérifications configurés dans le repository.
Continuous Deployment

Une fois les modifications validées, le workflow peut déployer le bundle vers l'environnement cible.

databricks bundle deploy -t dev

Le même principe peut être appliqué au déploiement de l'environnement de production avec le target correspondant.
10. Principes DevOps appliqués

Le projet applique plusieurs principes DevOps au développement de la plateforme Data :

    code versionné avec Git ;

    configuration déclarative ;

    Infrastructure as Code ;

    environnements séparés ;

    validation automatisée ;

    CI/CD ;

    déploiements reproductibles ;

    automatisation des opérations Databricks.

L'objectif est de traiter le pipeline Data comme un logiciel industrialisé, et non comme une suite de notebooks configurés manuellement.
11. Technologies
Domaine	Technologie
Cloud Data Platform	Databricks
Processing	Apache Spark / PySpark
Streaming	Spark Structured Streaming
Ingestion	Auto Loader
Pipeline framework	Spark Declarative Pipelines
Storage	Delta Lake
Data Quality	SDP Expectations
CDC	Auto CDC
Historisation	SCD Type 2
Modélisation	Kimball / Star Schema
Deployment	Databricks Asset Bundles
IaC	DAB / configuration déclarative
CI/CD	GitHub Actions
Version Control	Git / GitHub
12. Structure du projet

end_to_end_dp/
│
├── .github/
│   └── workflows/
│       └── ...
│
├── resources/
│   └── ...
│
├── src/
│   └── ...
│
├── databricks.yml
│
└── README.md

Le code du pipeline, la configuration du bundle et les workflows CI/CD sont versionnés dans le même repository.
13. Flux complet

Le flux de traitement peut être résumé ainsi :

                    SOURCES
                       │
                       ▼
                  Auto Loader
                       │
                       ▼
                  ┌─────────┐
                  │ BRONZE  │
                  └────┬────┘
                       │
                       ▼
              Data Quality Rules
                       │
                       ▼
              Cleaning / Casting
                       │
                       ▼
                Deduplication
                       │
                       ▼
             ┌─────────────────┐
             │     SILVER      │
             │                 │
             │ Customers       │
             │ Orders          │
             └───────┬─────────┘
                     │
          ┌──────────┴──────────┐
          │                     │
       Auto CDC              Orders
          │                     │
          ▼                     │
       SCD Type 2               │
          │                     │
          └──────────┬──────────┘
                     │
                     ▼
                  GOLD
                     │
          ┌──────────┼───────────┐
          │          │           │
       Dimensions  Fact Orders    │
          │          │           │
          └──────────┼───────────┘
                     │
                     ▼
               BI / Analytics

14. Ce que démontre le projet

Ce projet couvre plusieurs problématiques rencontrées dans une plateforme Data moderne :

    ingestion incrémentale de plusieurs sources ;

    traitement streaming avec Spark ;

    gestion des données en retard ;

    déduplication ;

    contrôle de qualité ;

    pseudonymisation ;

    Change Data Capture ;

    historisation SCD Type 2 à partir d'événements CDC ;

    modélisation dimensionnelle ;

    construction d'une couche Gold destinée à l'analyse ;

    déploiement déclaratif des ressources Databricks ;

    Infrastructure as Code ;

    séparation des environnements ;

    CI/CD ;

    automatisation des déploiements.

L'ensemble permet de passer de données sources brutes à une couche analytique structurée tout en appliquant des pratiques d'industrialisation et de DevOps.
15. Commandes principales
Valider le bundle

databricks bundle validate -t dev

Déployer

databricks bundle deploy -t dev

Vérifier la configuration

databricks bundle summary -t dev

Auteur

Paul Coffi

Projet personnel de Data Engineering autour de la conception et de l'industrialisation d'une plateforme de données retail avec Databricks.
