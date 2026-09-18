# End-to-End Data Platform — Retail

## Présentation

Ce projet met en œuvre une plateforme de données retail de bout en bout sur **Databricks**.

L'objectif est de transformer des données opérationnelles provenant de plusieurs sources en un modèle analytique structuré, exploitable pour des besoins de reporting et de BI.

Le pipeline repose sur une **architecture Medallion** :

```text
Sources
   │
   ▼
Bronze
   │
   ▼
Silver
   │
   ▼
Gold
   │
   ▼
Analyse / BI
```

La partie Data Engineering est développée avec **PySpark** et **Spark Declarative Pipelines**.

La gestion et le déploiement du projet sont réalisés avec **Databricks Asset Bundles (DAB)**, tandis que **GitHub Actions** automatise le processus CI/CD.

---

# Architecture globale

Le projet combine deux dimensions :

1. **Le pipeline de données**, responsable de l'ingestion et de la transformation des données.
2. **Le Databricks Asset Bundle**, responsable de la gestion et du déploiement des ressources Databricks.

```text
                         GitHub
                            │
                            │ Pull Request / Push
                            ▼
                    ┌─────────────────┐
                    │  GitHub Actions │
                    │                 │
                    │ CI              │
                    │ Tests           │
                    │ Bundle Validate │
                    │                 │
                    │ CD              │
                    │ Bundle Deploy   │
                    └────────┬────────┘
                             │
                             ▼
                  ┌─────────────────────┐
                  │ Databricks Asset    │
                  │ Bundle              │
                  └──────────┬──────────┘
                             │
                             ▼
                     Databricks
                             │
                             ▼
                  Spark Declarative
                     Pipeline
                             │
                             ▼
        ┌─────────────────────────────────────┐
        │            Data Pipeline             │
        │                                     │
        │  Sources → Bronze → Silver → Gold   │
        └────────────────────┬────────────────┘
                             │
                             ▼
                         BI / SQL
```

---

# 1. Données sources

Le pipeline traite plusieurs flux de données représentant l'activité d'une entreprise de retail.

Deux grandes catégories de données sont utilisées :

* les changements concernant les clients ;
* les commandes provenant de plusieurs filiales.

## Données clients

Les modifications des clients sont déposées quotidiennement dans :

```text
customer_changes_daily/
```

Les fichiers sont au format **JSON**.

Les données contiennent notamment :

```text
customer_id
first_name
last_name
address
city
email
loyalty_tier
timestamp
operation
```

Le champ `operation` permet d'identifier les opérations réalisées sur les données clients.

## Données de commandes

Les commandes proviennent de trois filiales :

```text
subsidiary_daily_orders/
│
├── bright_home_orders/
├── lumina_sports_orders/
└── northstar_outfitters_orders/
```

Les formats sont volontairement hétérogènes :

| Source               | Format |
| -------------------- | ------ |
| Bright Homer         | CSV    |
| Lumina Sports        | CSV    |
| Northstar Outfitters | JSON   |

Le pipeline rassemble ces différents flux dans une table de commandes commune.

---

# 2. Architecture Medallion

Les données traversent trois couches.

```text
┌────────────────────────────────────────────┐
│                   SOURCES                  │
└──────────────────────┬─────────────────────┘
                       │
                       ▼
┌────────────────────────────────────────────┐
│                   BRONZE                   │
│                                            │
│  Ingestion des données brutes             │
│  Auto Loader / Structured Streaming        │
└──────────────────────┬─────────────────────┘
                       │
                       ▼
┌────────────────────────────────────────────┐
│                   SILVER                   │
│                                            │
│  Nettoyage                                 │
│  Data Quality                              │
│  Standardisation                           │
│  Déduplication                             │
│  CDC / SCD Type 2                          │
│  Pseudonymisation                          │
└──────────────────────┬─────────────────────┘
                       │
                       ▼
┌────────────────────────────────────────────┐
│                    GOLD                    │
│                                            │
│  Modèle dimensionnel Kimball              │
│  Dimensions                                │
│  Table de faits                            │
└──────────────────────┬─────────────────────┘
                       │
                       ▼
                  Analyse / BI
```

---

# 3. Couche Bronze

La couche Bronze constitue le point d'entrée des données dans le Lakehouse.

Les données sont ingérées avec **Spark Structured Streaming** et **Auto Loader**.

Les tables principales sont :

```text
sdp_data_pipeline.bronze.customers_bronze
sdp_data_pipeline.bronze.orders_bronze
```

## Clients

Les fichiers JSON provenant de `customer_changes_daily` sont ingérés avec Auto Loader.

```python
spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "json")
```

Les données sont conservées dans une table Bronze append-only.

## Commandes

Les trois filiales alimentent la même table :

```text
orders_bronze
```

```text
Bright Homer ───────┐
                    │
Lumina Sports ──────┼──► orders_bronze
                    │
Northstar ──────────┘
```

Cette ingestion utilise plusieurs `append_flow`.

Cela permet de regrouper les commandes provenant de sources différentes dans un flux commun.

---

# 4. Couche Silver

La couche Silver prépare les données pour les traitements analytiques.

Elle comprend deux flux principaux :

```text
customers_bronze ──► customers_silver

orders_bronze ─────► orders_silver
```

---

## 4.1 Clients

Les données clients passent par une vue temporaire :

```text
customers_vw
```

Cette vue applique plusieurs traitements avant leur intégration dans :

```text
sdp_data_pipeline.silver.customers_silver
```

### Data Quality

Une première règle vérifie la présence de `customer_id`.

```text
customer_id IS NOT NULL
```

Une deuxième règle vérifie que la ville est renseignée :

```text
city IS NOT NULL
```

Les données ne respectant pas ces règles sont exclues du traitement.

### Nettoyage

Le timestamp est converti en type `timestamp`.

Un watermark d'une heure est appliqué afin de gérer les événements arrivant avec retard.

Les doublons sont supprimés.

### Pseudonymisation

Certaines données personnelles sont hachées avec SHA-256 :

```text
first_name
last_name
address
email
```

La logique est :

```text
Donnée personnelle
       │
       ▼
    SHA-256
       │
       ▼
Valeur pseudonymisée
```

Le `loyalty_tier` est également nettoyé avec `trim()`.

---

# 5. CDC et SCD Type 2

Les modifications clients sont gérées avec le mécanisme **Auto CDC** de Spark Declarative Pipelines.

Le flux utilise :

```text
customer_id
```

comme clé et :

```text
timestamp
```

comme colonne de séquencement.

La table cible est stockée en **SCD Type 2**.

Cela permet de conserver l'évolution des informations d'un client dans le temps.

```text
Client 123

Version 1
    │
    │ modification
    ▼
Version 2
    │
    │ modification
    ▼
Version 3
```

Le pipeline conserve ainsi l'historique des différentes versions plutôt que de simplement écraser la valeur précédente.

---

# 6. Commandes Silver

La table :

```text
sdp_data_pipeline.silver.orders_silver
```

est construite à partir de :

```text
sdp_data_pipeline.bronze.orders_bronze
```

Plusieurs contrôles de qualité sont appliqués.

```text
order_id IS NOT NULL
qty > 0
unit_price > 0
discount_pct > 0
total_amount > 0
```

Les lignes ne respectant pas ces règles sont supprimées.

## Standardisation

Les champs textuels sont nettoyés avec `trim()` :

```text
category
channel
city
country
customer_id
region
sku
```

Les types sont ensuite explicitement définis :

```text
qty            → integer
order_date     → date
order_timestamp→ timestamp
unit_price     → decimal(12,2)
total_amount   → decimal(12,2)
```

Les commandes sont dédupliquées à partir de :

```text
order_id
```

---

# 7. Couche Gold

La couche Gold transforme les données Silver en **modèle dimensionnel Kimball**.

Le modèle est constitué de :

```text
dim_customer
dim_product
dim_date
dim_channel
fact_orders
```

Architecture :

```text
                         dim_customer
                              │
                              │
                              ▼
dim_date ───────────────► fact_orders ◄──────────── dim_product
                              ▲
                              │
                              │
                         dim_channel
```

---

# 8. Dimension Customer

```text
gold.dim_customer
```

Cette dimension est construite à partir des commandes Silver.

Elle contient :

```text
customer_id
region
country
city
```

Les clients sont dédupliqués sur `customer_id`.

Elle permet notamment d'analyser les ventes selon :

* la région ;
* le pays ;
* la ville ;
* le client.

---

# 9. Dimension Product

```text
gold.dim_product
```

Elle contient :

```text
product_key
sku
category
```

Les produits sont dédupliqués à partir de `sku`.

Une clé technique `product_key` est générée pour être utilisée dans la table de faits.

---

# 10. Dimension Date

```text
gold.dim_date
```

Cette dimension permet d'effectuer des analyses temporelles.

Elle contient notamment :

```text
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
```

La clé de date est générée au format :

```text
yyyyMMdd
```

Exemple :

```text
2026-09-18
     ↓
20260918
```

Cette dimension permet notamment d'analyser les ventes par :

* année ;
* trimestre ;
* mois ;
* jour ;
* jour de la semaine ;
* semaine ;
* semaine/week-end.

---

# 11. Dimension Channel

```text
gold.dim_channel
```

Elle représente les canaux de vente.

Elle contient :

```text
channel_key
channel
```

Les valeurs sont dédupliquées et une clé technique est générée.

---

# 12. Table de faits

La table centrale du modèle est :

```text
gold.fact_orders
```

Elle est construite à partir de `orders_silver`.

Les clés des dimensions sont récupérées par jointure :

```text
orders_silver
      │
      ├────► dim_product ──► product_key
      │
      └────► dim_channel ──► channel_key
```

La clé de date est dérivée directement de `order_date`.

La table contient notamment :

```text
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

La table de faits permet donc de relier les transactions aux différentes dimensions analytiques.

---

# 13. Modèle analytique

Le modèle final permet de réaliser des analyses multidimensionnelles des ventes.

```text
                       Customer
                           │
                           │
                           ▼
                       ┌─────────┐
                       │         │
             Date ────►│ Orders  │◄──── Product
                       │         │
                       └────┬────┘
                            │
                            ▼
                         Channel
```

Il devient ainsi possible d'analyser :

* le chiffre d'affaires ;
* les quantités vendues ;
* les ventes par produit ;
* les ventes par catégorie ;
* les ventes par canal ;
* les ventes par client ;
* les ventes par zone géographique ;
* l'évolution des ventes dans le temps.

---

# 14. Spark Declarative Pipelines

Le pipeline est défini avec **Spark Declarative Pipelines**.

Les différentes ressources sont déclarées directement dans le code Python.

Par exemple :

```python
@dp.table(...)
def customers_bronze():
    ...
```

ou :

```python
@dp.materialized_view(...)
def dim_product():
    ...
```

Le pipeline décrit donc les relations entre les différentes tables plutôt qu'une succession manuelle d'étapes d'exécution.

Le graphe logique est :

```text
customers_bronze ──► customers_vw ──► customers_silver
                                             
orders_bronze ───────────────────────► orders_silver
                                             │
                                             ▼
                                      ┌─────────────┐
                                      │    GOLD     │
                                      │             │
                                      │ Dimensions  │
                                      │ + Fact      │
                                      └─────────────┘
```

---

# 15. Databricks Asset Bundle

Le pipeline n'est pas géré uniquement depuis l'interface Databricks.

Le projet est organisé comme un **Databricks Asset Bundle (DAB)**.

Le bundle permet de définir les ressources Databricks sous forme de code et de les versionner avec Git.

La structure du projet suit notamment cette logique :

```text
end_to_end_dp/
│
├── databricks.yml
│
├── resources/
│   └── ...
│
├── src/
│   └── retail_pipeline/
│       └── transformations/
│           ├── bronze
│           ├── silver
│           └── gold
│
└── .github/
    └── workflows/
        └── ...
```

Le fichier :

```text
databricks.yml
```

constitue le point d'entrée du bundle.

---

# 16. Gestion des environnements

Les environnements sont définis dans le DAB à travers des `targets`.

Par exemple :

```yaml
targets:
  dev:
    mode: development

  prod:
    mode: production
```

Le même code peut ainsi être déployé vers différents environnements.

```text
                 Git
                  │
                  ▼
           Databricks Bundle
                  │
          ┌───────┴───────┐
          │               │
          ▼               ▼
        DEV             PROD
          │               │
          ▼               ▼
     Databricks        Databricks
```

---

# 17. Déploiement du pipeline

Le bundle peut être validé avec :

```bash
databricks bundle validate -t dev
```

Cette commande vérifie la configuration du bundle sans déployer les ressources.

Le déploiement est ensuite effectué avec :

```bash
databricks bundle deploy -t dev
```

Le bundle crée ou met à jour les ressources Databricks définies dans le projet.

Il est également possible de consulter le résumé du déploiement avec :

```bash
databricks bundle summary -t dev
```

---

# 18. CI/CD avec GitHub Actions

Le projet utilise GitHub Actions afin d'automatiser la validation et le déploiement du DAB.

Le principe est :

```text
                    Pull Request
                         │
                         ▼
                 ┌──────────────┐
                 │      CI      │
                 │              │
                 │ Tests        │
                 │ Bundle      │
                 │ Validate     │
                 └──────┬───────┘
                        │
                      Merge
                        │
                        ▼
                      main
                        │
                        ▼
                 ┌──────────────┐
                 │      CD      │
                 │              │
                 │ Bundle       │
                 │ Deploy       │
                 └──────┬───────┘
                        │
                        ▼
                   Databricks
```

## CI

Lors d'une Pull Request, le pipeline peut effectuer :

```bash
pytest
databricks bundle validate -t dev
```

Cela permet de détecter les erreurs avant l'intégration du code.

## CD

Après intégration dans `main`, le bundle est automatiquement déployé :

```bash
databricks bundle deploy -t dev
```

Le déploiement devient ainsi reproductible et versionné.

---

# 19. Structure complète du projet

```text
end_to_end_dp/
│
├── databricks.yml
│
├── resources/
│   └── retail_pipeline.yml
│
├── src/
│   └── retail_pipeline/
│       │
│       └── transformations/
│           │
│           ├── bronze/
│           │   └── ...
│           │
│           ├── silver/
│           │   └── ...
│           │
│           └── gold/
│               └── ...
│
├── tests/
│   └── ...
│
└── .github/
    └── workflows/
        └── databricks.yml
```

---

# 20. Workflow de développement

Le développement du projet suit un workflow Git classique :

```text
1. Création d'une branche
          │
          ▼
2. Développement du pipeline
          │
          ▼
3. Tests
          │
          ▼
4. Push Git
          │
          ▼
5. Pull Request
          │
          ▼
6. CI
   ├── Tests
   └── Bundle Validate
          │
          ▼
7. Merge dans main
          │
          ▼
8. CD
   └── Bundle Deploy
          │
          ▼
9. Databricks
          │
          ▼
10. Exécution du pipeline
```

---

# 21. Ce que le projet met en pratique

Ce projet couvre plusieurs aspects d'une plateforme Data moderne :

### Data Engineering

* Spark Structured Streaming ;
* Auto Loader ;
* Spark Declarative Pipelines ;
* Delta Lake ;
* ingestion incrémentale ;
* traitement de plusieurs sources ;
* data quality ;
* déduplication ;
* watermarking ;
* Change Data Capture ;
* SCD Type 2.

### Data Modeling

* architecture Medallion ;
* modélisation Kimball ;
* schéma en étoile ;
* tables de faits ;
* dimensions ;
* clés techniques ;
* dimension temporelle.

### Data Governance

* pseudonymisation des données personnelles ;
* séparation des couches ;
* organisation avec Unity Catalog.

### DevOps / DataOps

* Git ;
* Databricks Asset Bundles ;
* Infrastructure as Code ;
* gestion des environnements ;
* GitHub Actions ;
* CI/CD ;
* déploiement automatisé.

---

# 22. Résumé du flux

Le projet peut finalement être résumé par le flux suivant :

```text
                 DONNÉES RETAIL
                       │
          ┌────────────┴────────────┐
          │                         │
   Customer Changes          Subsidiary Orders
          │                         │
          │                 ┌───────┼────────┐
          │                 │       │        │
          │              Bright   Lumina   Northstar
          │              Homer    Sports   Outfitters
          │                 │       │        │
          └─────────────────┼───────┼────────┘
                            │
                            ▼
                       ┌─────────┐
                       │ BRONZE  │
                       └────┬────┘
                            │
                            ▼
                       ┌─────────┐
                       │ SILVER  │
                       │         │
                       │ Quality │
                       │ CDC     │
                       │ SCD2    │
                       │ PII     │
                       └────┬────┘
                            │
                            ▼
                       ┌─────────┐
                       │  GOLD   │
                       │         │
                       │ Kimball │
                       │         │
                       │  Facts  │
                       │  Dims   │
                       └────┬────┘
                            │
                            ▼
                       ANALYSE / BI


              ┌─────────────────────────┐
              │ Databricks Asset Bundle │
              │                         │
              │ Configuration           │
              │ Resources               │
              │ Environments            │
              │ Deployment              │
              └────────────┬────────────┘
                           │
                           ▼
                    GitHub Actions
                           │
                    ┌──────┴──────┐
                    │             │
                   CI             CD
                    │             │
               Validate        Deploy
               + Tests           │
                                  ▼
                             Databricks
```

---

# 23. Objectif du projet

L'objectif du projet est de mettre en œuvre une chaîne Data complète, depuis l'ingestion de données opérationnelles hétérogènes jusqu'à leur exposition sous forme de modèle analytique.

Au-delà des transformations Spark, le projet met également en pratique la **gestion du cycle de vie d'une plateforme Databricks** avec :

* du code versionné ;
* une définition déclarative des pipelines ;
* des ressources gérées par Databricks Asset Bundles ;
* une séparation des environnements ;
* une validation automatisée ;
* un déploiement automatisé avec CI/CD.

Le projet reproduit ainsi un workflow combinant **Data Engineering, Data Modeling et DataOps** au sein d'une même plateforme.
