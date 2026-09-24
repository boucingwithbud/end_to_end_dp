# End-to-End Retail Data Platform

Plateforme Data retail construite avec Databricks, Apache Spark et Spark Declarative Pipelines (SDP), avec une approche streaming, CDC et déploiement industrialisé.

Le projet met en œuvre un traitement incrémental basé sur Spark Structured Streaming, depuis l'ingestion de fichiers JSON/CSV jusqu'à la construction d'un modèle analytique en étoile.

Le SCD Type 2 est implémenté à partir d'événements Change Data Capture (CDC) avec Auto CDC. Le pipeline ne repose donc pas sur une comparaison périodique de snapshots pour détecter les changements clients.

L'ensemble du projet est versionné et déployable avec **Databricks Asset Bundles (DAB)**, avec une chaîne CI/CD GitHub Actions permettant de valider et déployer les ressources Databricks.

## Sommaire

1. [Architecture](#architecture)
2. [Ingestion — Bronze](#1-ingestion--bronze)
3. [Transformation — Silver](#2-transformation--silver)
4. [Change Data Capture et SCD Type 2](#3-change-data-capture-et-scd-type-2)
5. [Orders — Silver](#4-orders--silver)
6. [Gold — Modèle analytique](#5-gold--modèle-analytique)
7. [Pourquoi Spark Declarative Pipelines ?](#6-pourquoi-spark-declarative-pipelines-)
8. [Databricks Asset Bundles](#7-databricks-asset-bundles)
9. [Infrastructure as Code](#8-infrastructure-as-code)
10. [CI/CD avec GitHub Actions](#9-cicd-avec-github-actions)
11. [Principes DevOps appliqués](#10-principes-devops-appliqués)
12. [Technologies](#11-technologies)
13. [Structure du projet](#12-structure-du-projet)
14. [Flux complet](#13-flux-complet)
15. [Ce que démontre le projet](#14-ce-que-démontre-le-projet)
16. [Commandes principales](#15-commandes-principales)

## Architecture

```
                                      SOURCES
                                         |
                      +------------------+------------------+
                      |                                     |
                Customer JSON                       Orders CSV / JSON
                      |                                     |
                      +------------------+------------------+
                                         |
                                         v
                                  +-------------+
                                  | Auto Loader |
                                  |  Streaming  |
                                  +------+------+
                                         |
                                         v
                                  +--------------+
                                  |    BRONZE    |
                                  |              |
                                  | customers    |
                                  | orders       |
                                  +------+-------+
                                         |
                                         v
                                  +--------------+
                                  |    SILVER    |
                                  |              |
                                  | Data Quality |
                                  | Deduplication|
                                  | Normalisation|
                                  | CDC / SCD2   |
                                  +------+-------+
                                         |
                                         v
                                  +--------------+
                                  |     GOLD     |
                                  |              |
                                  | dim_customer |
                                  | dim_product  |
                                  | dim_date     |
                                  | dim_channel  |
                                  | fact_orders  |
                                  +------+-------+
                                         |
                                         v
                                  BI / Analytics
```

**Point clé : CDC → SCD Type 2**

```
Customer events
      |
      v
   BRONZE
      |
      v
customers_vw
      |
      v
   Auto CDC
      |
      v
 SCD Type 2
      |
      v
customers_silver
```

## 1. Ingestion — Bronze

Les données sont déposées dans des volumes Databricks sous forme de fichiers JSON et CSV. Les événements de changement client sont ingérés avec les informations nécessaires au traitement CDC.

Le pipeline utilise Auto Loader avec `cloudFiles` pour détecter et traiter progressivement les nouveaux fichiers.

### Customers

Les changements clients sont ingérés depuis des fichiers JSON :

```python
spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .load(...)
```

Les données sont conservées dans :

```
sdp_data_pipeline.bronze.customers_bronze
```

La table Bronze est configurée comme une table append-only afin de conserver les événements entrants.

### Orders

Les commandes provenant de plusieurs filiales sont ingérées vers une même table Bronze.

```
Bright Home Orders (CSV)         Lumina Sports (CSV)      Northstar Outfitters (JSON)
              \                          |                          /
               \                         |                         /
                +------------------------+------------------------+
                                         |
                                         v
                                  orders_bronze
```

Cette approche permet de centraliser les événements de commandes provenant de plusieurs systèmes sources. Les trois flux alimentent :

```
sdp_data_pipeline.bronze.orders_bronze
```

## 2. Transformation — Silver

La couche Silver constitue la couche de préparation et de qualité des données.

Elle réalise notamment :

- validation des données
- filtrage des enregistrements invalides
- déduplication
- gestion des événements en retard
- normalisation des chaînes
- conversion des types
- pseudonymisation de certaines données clients
- gestion des changements clients avec CDC

### Customers

Les données clients sont consommées en streaming depuis Bronze. Avant d'être appliquées à la table Silver, plusieurs règles sont exécutées.

**Data quality**

```python
@dp.expect_or_drop("valid_id", "customer_id IS NOT NULL")
```

Les enregistrements ne contenant pas de `customer_id` sont rejetés. Une autre règle vérifie que la ville est renseignée :

```python
customers_rule = {
    "valid_city": "city IS NOT NULL"
}
```

Les données sont ensuite nettoyées et dédupliquées.

**Gestion des données en retard**

Le pipeline utilise un watermark :

```python
.withWatermark("timestamp", "1 hours")
```

Cela permet de définir une fenêtre de tolérance pour les événements arrivant en retard, tout en permettant à Spark de gérer l'état du streaming. La déduplication est ensuite réalisée avant l'application des changements.

**Pseudonymisation**

Certaines données personnelles sont transformées avant leur exposition dans la couche Silver :

```python
.withColumn("first_name", F.sha2(F.col("first_name"), 256))
.withColumn("last_name", F.sha2(F.col("last_name"), 256))
.withColumn("email", F.sha2(F.col("email"), 256))
```

Les valeurs originales ne sont donc pas directement propagées vers les couches analytiques.

## 3. Change Data Capture et SCD Type 2

Le SCD Type 2 est implémenté à partir d'événements CDC.

Contrairement à un snapshot classique, le pipeline reçoit directement les changements à appliquer : insertions, mises à jour et suppressions. Ces événements sont ensuite ordonnés et appliqués à la table Silver afin de conserver l'historique des versions d'un client.

```
Bronze
   |
   v
customers_vw
   |
   v
Auto CDC
   |
   v
customers_silver
```

Le pipeline utilise :

```python
dp.create_auto_cdc_flow(
    target="sdp_data_pipeline.silver.customers_silver",
    source="customers_vw",
    keys=["customer_id"],
    sequence_by="timestamp",
    stored_as_scd_type=2
)
```

| Élément | Rôle |
|---|---|
| `customer_id` | clé métier du client |
| `timestamp` | ordre d'application des événements |
| `stored_as_scd_type=2` | conservation de l'historique |
| Auto CDC | application des changements CDC |

Les changements sont ainsi conservés en SCD Type 2, ce qui permet de préserver l'historique des versions d'un client.

## 4. Orders — Silver

Les commandes sont également traitées en streaming depuis Bronze.

Les règles de qualité vérifient notamment :

```sql
order_id IS NOT NULL
qty > 0
unit_price > 0
discount_pct > 0
total_amount > 0
```

Les données sont ensuite normalisées :

| Champ | Transformation |
|---|---|
| String | trim → normalisation |
| Timestamp | cast timestamp |
| Quantity | integer |
| Price / Amount | decimal(12,2) |
| Order date | date |

La déduplication des commandes est réalisée à partir de `order_id`.

La table obtenue est :

```
sdp_data_pipeline.silver.orders_silver
```

## 5. Gold — Modèle analytique

La couche Gold transforme les données Silver en un modèle dimensionnel de type Kimball.

Le modèle est organisé autour d'une table de faits et de plusieurs dimensions.

```
                    dim_customer
                         |
                         v
dim_date  --------> fact_orders <------ dim_product
                         |
                         v
                    dim_channel
```

### Fact table — fact_orders

La table de faits contient les événements de commande :

```
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
```

Les clés des dimensions sont récupérées lors de la construction de la table de faits.

### Dimensions

**dim_customer** — dimension client construite à partir des données de commandes :
`customer_id, region, country, city`

**dim_product** — dimension produit basée sur `sku`, `category`, avec une clé surrogate `product_key`.

**dim_date** — dimension calendrier :
`date_key, date, year, quarter, month, month_name, day, day_of_week, day_name, week_of_year, is_weekend`

Cette dimension facilite les analyses temporelles.

**dim_channel** — dimension représentant les différents canaux de vente :
`channel_key, channel`

## 6. Pourquoi Spark Declarative Pipelines ?

Le pipeline utilise Spark Declarative Pipelines pour déclarer les différentes tables, vues et flows du pipeline.

La logique est organisée autour des dépendances entre les datasets :

```
Bronze
  |
  v
Silver
  |
  +-- customers
  |      |
  |      v
  |   Auto CDC
  |      |
  |      v
  |    SCD2
  |
  +-- orders
         |
         v
       Gold
         |
         +-- dimensions
         +-- fact_orders
```

Cette approche permet de décrire la logique du pipeline sans avoir à gérer manuellement l'ordre d'exécution de chaque étape.

## 7. Databricks Asset Bundles

**Databricks Asset Bundles (DAB)** est l'outil de packaging et de déploiement utilisé pour l'ensemble du projet. Il permet de traiter la configuration Databricks (pipelines, jobs, clusters, permissions) comme du **code versionné**, plutôt que comme des objets configurés manuellement dans l'interface Databricks.

Concrètement, un bundle DAB regroupe dans un même repository :

- le fichier de configuration racine `databricks.yml` ;
- la définition des ressources (pipelines SDP, jobs, workflows) dans `resources/` ;
- le code source du pipeline dans `src/` ;
- la déclaration des environnements cibles (targets), par exemple `dev` et `prod`.

```
Git repository
       |
       v
Databricks Asset Bundle
       |
       +-- configuration (databricks.yml)
       +-- pipeline (SDP)
       +-- jobs / resources
       +-- environnements (targets)
```

Le bundle est ensuite validé et déployé avec la CLI Databricks (`databricks bundle validate`, `databricks bundle deploy`), ce qui permet de reproduire à l'identique le même déploiement sur plusieurs environnements.

### Environnements (targets)

DAB permet de définir plusieurs targets dans `databricks.yml`, chacun avec sa propre configuration (workspace, chemins, paramètres du pipeline) :

```
                Git
                 |
                 v
          Databricks Bundle
             /            \
           DEV             PROD
```

Le target `dev` est utilisé pour le développement et la validation du projet. Les configurations spécifiques aux environnements sont définies directement dans le bundle plutôt que dupliquées dans le code du pipeline.

## 8. Infrastructure as Code

La configuration des ressources Databricks est versionnée dans Git. Cela permet de rapprocher le projet d'une approche Infrastructure as Code (IaC) :

```
Code + Configuration + Databricks resources
              |
              v
             Git
              |
              v
   Reproductible / versionné
```

L'objectif est de pouvoir recréer ou modifier l'environnement Databricks à partir de fichiers versionnés plutôt que de dépendre exclusivement de configurations manuelles dans le workspace.

Databricks Asset Bundles joue ici le rôle de couche de déploiement et de gestion de configuration des ressources Databricks.

## 9. CI/CD avec GitHub Actions

Le repository utilise GitHub Actions pour automatiser la validation et le déploiement.

Le workflow suit une logique de type :

```
Developer
    |
    v
Git push / Pull Request
    |
    v
GitHub Actions
    |
    +-- Validation
    +-- Tests
    +-- databricks bundle validate
             |
             v
        Deployment
             |
             v
        Databricks
```

**Continuous Integration**

La CI vérifie que les modifications proposées peuvent être intégrées au projet, notamment via :

```bash
databricks bundle validate -t dev
```

ainsi que les tests et vérifications configurés dans le repository.

**Continuous Deployment**

Une fois les modifications validées, le workflow peut déployer le bundle vers l'environnement cible :

```bash
databricks bundle deploy -t dev
```

Le même principe peut être appliqué au déploiement de l'environnement de production avec le target correspondant.

## 10. Principes DevOps appliqués

Le projet applique plusieurs principes DevOps au développement de la plateforme Data :

- code versionné avec Git
- configuration déclarative
- Infrastructure as Code
- environnements séparés
- validation automatisée
- CI/CD
- déploiements reproductibles
- automatisation des opérations Databricks

L'objectif est de traiter le pipeline Data comme un logiciel industrialisé, et non comme une suite de notebooks configurés manuellement.

## 11. Technologies

| Domaine | Technologie |
|---|---|
| Cloud Data Platform | Databricks |
| Processing | Apache Spark / PySpark |
| Streaming | Spark Structured Streaming |
| Ingestion | Auto Loader |
| Pipeline framework | Spark Declarative Pipelines |
| Storage | Delta Lake |
| Data Quality | SDP Expectations |
| CDC | Auto CDC |
| Historisation | SCD Type 2 |
| Modélisation | Kimball / Star Schema |
| Deployment | Databricks Asset Bundles (DAB) |
| IaC | DAB / configuration déclarative |
| CI/CD | GitHub Actions |
| Version Control | Git / GitHub |

## 12. Structure du projet

```
end_to_end_dp/
|
+-- .github/
|   +-- workflows/
|       +-- ...
|
+-- resources/
|   +-- ...
|
+-- src/
|   +-- ...
|
+-- databricks.yml
|
+-- README.md
```

Le code du pipeline, la configuration du bundle (DAB) et les workflows CI/CD sont versionnés dans le même repository.

## 13. Flux complet

Le flux de traitement peut être résumé ainsi :

```
                         SOURCES
                            |
                            v
                       Auto Loader
                            |
                            v
                     +------------+
                     |   BRONZE   |
                     +-----+------+
                           |
                           v
                  Data Quality Rules
                           |
                           v
                  Cleaning / Casting
                           |
                           v
                    Deduplication
                           |
                           v
                   +--------------+
                   |    SILVER    |
                   |              |
                   |  Customers   |
                   |  Orders      |
                   +------+-------+
                          |
              +-----------+-----------+
              |                       |
           Auto CDC                Orders
              |                       |
              v                       |
         SCD Type 2                   |
              |                       |
              +-----------+-----------+
                          |
                          v
                        GOLD
                          |
               +----------+----------+
               |          |          |
          Dimensions  Fact Orders    |
               |          |          |
               +----------+----------+
                          |
                          v
                    BI / Analytics
```

## 14. Ce que démontre le projet

Ce projet couvre plusieurs problématiques rencontrées dans une plateforme Data moderne :

- ingestion incrémentale de plusieurs sources
- traitement streaming avec Spark
- gestion des données en retard
- déduplication
- contrôle de qualité
- pseudonymisation
- Change Data Capture
- historisation SCD Type 2 à partir d'événements CDC
- modélisation dimensionnelle
- construction d'une couche Gold destinée à l'analyse
- déploiement déclaratif des ressources Databricks avec Databricks Asset Bundles
- Infrastructure as Code
- séparation des environnements
- CI/CD
- automatisation des déploiements

L'ensemble permet de passer de données sources brutes à une couche analytique structurée tout en appliquant des pratiques d'industrialisation et de DevOps.

## 15. Commandes principales

Valider le bundle :

```bash
databricks bundle validate -t dev
```

Déployer :

```bash
databricks bundle deploy -t dev
```

Vérifier la configuration :

```bash
databricks bundle summary -t dev
```

---

**Auteur : Paul Coffi**

Projet personnel de Data Engineering autour de la conception et de l'industrialisation d'une plateforme de données retail avec Databricks.
