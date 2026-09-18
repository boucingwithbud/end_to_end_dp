## Présentation

Ce projet met en œuvre une plateforme de données retail de bout en bout sur **Databricks**, avec un pipeline de traitement **incrémental et orienté streaming**, plutôt qu'un traitement batch traditionnel.

L'objectif est de transformer des données opérationnelles provenant de plusieurs sources en un modèle analytique structuré, exploitable pour des besoins de reporting et de BI.

Le pipeline repose sur une **architecture Medallion** :

```text
Sources
   │
   │ Flux de données
   ▼
Bronze
   │
   │ Traitement incrémental
   ▼
Silver
   │
   │ Transformations incrémentales
   ▼
Gold
   │
   ▼
Analyse / BI
```

La partie Data Engineering est développée avec **PySpark**, **Spark Structured Streaming**, **Auto Loader** et **Spark Declarative Pipelines**.

Les données sont traitées au fil de leur arrivée grâce aux mécanismes de streaming de Spark, avec notamment la gestion des événements en retard, la déduplication et l'Auto CDC pour maintenir les changements clients.

La gestion et le déploiement du projet sont réalisés avec **Databricks Asset Bundles (DAB)**, tandis que **GitHub Actions** automatise le processus CI/CD.
