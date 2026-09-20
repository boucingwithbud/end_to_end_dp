# End-to-End Retail Data Platform

Pipeline de données retail construit avec **Databricks et Spark Declarative Pipelines (SDP)**.

Le projet met en œuvre un traitement **incrémental basé sur Spark Structured Streaming**, depuis l'ingestion de fichiers JSON/CSV jusqu'à la construction d'un modèle analytique en étoile.

L'ensemble du projet est versionné et déployable avec **Databricks Asset Bundles (DAB)**, avec une chaîne **CI/CD GitHub Actions** permettant de valider et déployer les ressources Databricks.

---

## Architecture

```text
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
```

---

# 1. Ingestion — Bronze

Les données sont déposées dans des volumes Databricks sous forme de fichiers JSON et CSV.

Le pipeline utilise **Auto Loader** avec `cloudFiles` pour détecter et traiter progressivement les nouveaux fichiers.

### Customers

Les changements clients sont ingérés depuis des fichiers JSON :

```python
spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .load(...)
```

Les données sont conservées dans :

```text
sdp_data_pipeline.bronze.customers_bronze
```

La table Bronze est configurée comme une table append-only afin de conserver les événements entrants.

### Orders

Les commandes provenant de plusieurs filiales sont ingérées vers une même table Bronze.

```text
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
```

Les différentes sources utilisent des formats différents :

* Bright Home : CSV
* Lumina Sports : CSV
* Northstar Outfitters : JSON

Les trois flux alimentent :

```text
sdp_data_pipeline.bronze.orders_bronze
```

Cette approche permet de centraliser les événements de commandes provenant de plusieurs systèmes sources.

---

# 2. Transformation — Silver

La couche Silver constitue la couche de préparation et de qualité des données.

Elle réalise notamment :

* validation des données ;
* filtrage des enregistrements invalides ;
* déduplication ;
* gestion des événements en retard ;
* normalisation des chaînes ;
* conversion des types ;
* pseudonymisation de certaines données clients ;
* gestion des changements clients avec CDC.

---

## Customers

Les données clients sont consommées en streaming depuis Bronze.

Avant d'être appliquées à la table Silver, plusieurs règles sont exécutées.

### Data quality

Exemple :

```python
@dp.expect_or_drop("valid_id", "customer_id IS NOT NULL")
```

Les enregistrements ne contenant pas de `customer_id` sont rejetés.

Une autre règle vérifie que la ville est renseignée :

```python
customers_rule = {
    "valid_city": "city IS NOT NULL"
}
```

Les données sont ensuite nettoyées et dédupliquées.

---

## Gestion des données en retard

Le pipeline utilise un watermark :

```python
.withWatermark("timestamp", "1 hours")
```

Cela permet de définir une fenêtre de tolérance pour les événements arrivant en retard tout en permettant à Spark de gérer l'état du streaming.

La déduplication est ensuite réalisée avant l'application des changements.

---

## Pseudonymisation

Certaines données personnelles sont transformées avant leur exposition dans la couche Silver.

Par exemple :

```python
.withColumn("first_name", F.sha2(F.col("first_name"), 256))
.withColumn("last_name", F.sha2(F.col("last_name"), 256))
.withColumn("email", F.sha2(F.col("email"), 256))
```

Les valeurs originales ne sont donc pas directement propagées vers les couches analytiques.

---

# 3. Change Data Capture et SCD Type 2

Les changements des clients sont gérés avec **Auto CDC**.

```text
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

La clé métier est :

```text
customer_id
```

et les événements sont ordonnés selon :

```text
timestamp
```

Les changements sont conservés en **SCD Type 2**, ce qui permet de préserver l'historique des versions d'un client.

---

# 4. Orders — Silver

Les commandes sont également traitées en streaming depuis Bronze.

Les règles de qualité vérifient notamment :

```text
order_id IS NOT NULL
qty > 0
unit_price > 0
discount_pct > 0
total_amount > 0
```

Les données sont ensuite normalisées :

```text
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
```

La déduplication des commandes est réalisée à partir de :

```text
order_id
```

La table obtenue est :

```text
sdp_data_pipeline.silver.orders_silver
```

---

# 5. Gold — Modèle analytique

La couche Gold transforme les données Silver en un modèle dimensionnel de type **Kimball**.

Le modèle est organisé autour d'une table de faits et de plusieurs dimensions.

```text
                    dim_customer
                         │
                         │
                         ▼
dim_date ───────► fact_orders ◄────── dim_product
                         │
                         │
                         ▼
                    dim_channel
```

---

## Fact table

### `fact_orders`

La table de faits contient les événements de commande.

Elle contient notamment :

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

Les clés des dimensions sont récupérées lors de la construction de la table de faits.

---

## Dimensions

### `dim_customer`

Dimension client construite à partir des données de commandes.

```text
customer_id
region
country
city
```

### `dim_product`

Dimension produit basée sur :

```text
sku
category
```

avec une clé surrogate :

```text
product_key
```

### `dim_date`

Dimension calendrier contenant notamment :

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

Cette dimension facilite les analyses temporelles.

### `dim_channel`

Dimension représentant les différents canaux de vente :

```text
channel_key
channel
```

---

# 6. Pourquoi Spark Declarative Pipelines ?

Le pipeline utilise **Spark Declarative Pipelines** pour déclarer les différentes tables, vues et flows du pipeline.

La logique est organisée autour des dépendances entre les datasets :

```text
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
```

Cette approche permet de décrire la logique du pipeline sans avoir à gérer manuellement l'ordre d'exécution de chaque étape.

---

# 7. Databricks Asset Bundles

Les ressources Databricks sont déployées avec **Databricks Asset Bundles (DAB)**.

Le bundle permet de traiter la configuration Databricks comme du code versionné.

```text
Git repository
       │
       ▼
Databricks Asset Bundle
       │
       ├── configuration
       ├── pipeline
       ├── jobs / resources
       └── environments
```

Les ressources du projet peuvent ainsi être déployées de manière reproductible plutôt que configurées manuellement dans l'interface Databricks.

---

## Environnements

Le projet distingue les environnements de déploiement.

```text
                Git
                 │
                 ▼
          Databricks Bundle
             /       \
            /         \
         DEV           PROD
```

Le target `dev` est utilisé pour le développement et la validation du projet.

Les configurations spécifiques aux environnements sont définies dans le bundle plutôt que dupliquées dans le code du pipeline.

---

# 8. Infrastructure as Code

La configuration des ressources Databricks est versionnée dans Git.

Cela permet de rapprocher le projet d'une approche **Infrastructure as Code (IaC)** :

```text
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
```

L'objectif est de pouvoir recréer ou modifier l'environnement Databricks à partir de fichiers versionnés plutôt que de dépendre exclusivement de configurations manuelles dans le workspace.

Databricks Asset Bundles joue ici le rôle de couche de déploiement et de gestion de configuration des ressources Databricks.

---

# 9. CI/CD avec GitHub Actions

Le repository utilise **GitHub Actions** pour automatiser la validation et le déploiement.

Le workflow suit une logique de type :

```text
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
```

### Continuous Integration

La CI vérifie que les modifications proposées peuvent être intégrées au projet.

Exemples :

```bash
databricks bundle validate -t dev
```

ainsi que les tests et vérifications configurés dans le repository.

### Continuous Deployment

Une fois les modifications validées, le workflow peut déployer le bundle vers l'environnement cible.

```bash
databricks bundle deploy -t dev
```

Le même principe peut être appliqué au déploiement de l'environnement de production avec le target correspondant.

---

# 10. Principes DevOps appliqués

Le projet applique plusieurs principes DevOps au développement de la plateforme Data :

* code versionné avec Git ;
* configuration déclarative ;
* Infrastructure as Code ;
* environnements séparés ;
* validation automatisée ;
* CI/CD ;
* déploiements reproductibles ;
* automatisation des opérations Databricks.

L'objectif est de traiter le pipeline Data comme un **logiciel industrialisé**, et non comme une suite de notebooks configurés manuellement.

---

# 11. Technologies

| Domaine             | Technologie                     |
| ------------------- | ------------------------------- |
| Cloud Data Platform | Databricks                      |
| Processing          | Apache Spark / PySpark          |
| Streaming           | Spark Structured Streaming      |
| Ingestion           | Auto Loader                     |
| Pipeline framework  | Spark Declarative Pipelines     |
| Storage             | Delta Lake                      |
| Data Quality        | SDP Expectations                |
| CDC                 | Auto CDC                        |
| Historisation       | SCD Type 2                      |
| Modélisation        | Kimball / Star Schema           |
| Deployment          | Databricks Asset Bundles        |
| IaC                 | DAB / configuration déclarative |
| CI/CD               | GitHub Actions                  |
| Version Control     | Git / GitHub                    |

---

# 12. Structure du projet

```text
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
```

Le code du pipeline, la configuration du bundle et les workflows CI/CD sont versionnés dans le même repository.

---

# 13. Flux complet

Le flux de traitement peut être résumé ainsi :

```text
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
```

---

# 14. Ce que démontre le projet

Ce projet couvre plusieurs problématiques rencontrées dans une plateforme Data moderne :

* ingestion incrémentale de plusieurs sources ;
* traitement streaming avec Spark ;
* gestion des données en retard ;
* déduplication ;
* contrôle de qualité ;
* pseudonymisation ;
* Change Data Capture ;
* historisation SCD Type 2 ;
* modélisation dimensionnelle ;
* construction d'une couche Gold destinée à l'analyse ;
* déploiement déclaratif des ressources Databricks ;
* Infrastructure as Code ;
* séparation des environnements ;
* CI/CD ;
* automatisation des déploiements.

L'ensemble permet de passer de données sources brutes à une couche analytique structurée tout en appliquant des pratiques d'industrialisation et de DevOps.

---

# 15. Commandes principales

### Valider le bundle

```bash
databricks bundle validate -t dev
```

### Déployer

```bash
databricks bundle deploy -t dev
```

### Vérifier la configuration

```bash
databricks bundle summary -t dev
```

---

## Auteur

**Paul Coffi**

Projet personnel de Data Engineering autour de la conception et de l'industrialisation d'une plateforme de données retail avec Databricks.
