# BikeDemand Predictor

Projet Big Data : Prédiction de la demande de vélos en libre-service.

## Semaine 1 : Infrastructure et Ingestion
- **Cluster Docker** : 1 Master Spark, 2 Workers, HDFS (1 NameNode, 2 DataNodes), Kafka.
- **Ingestion** : Script `producer.py` qui lit `hour.csv` et l'envoie dans le topic Kafka `bike-demand-events`.

## Comment lancer le projet
1. Démarrer le cluster : `docker compose up -d`
2. Lancer le producteur (dans le dossier ingestion) : `python producer.py`
