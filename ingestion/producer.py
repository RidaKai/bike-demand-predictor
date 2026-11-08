import csv
import json
import time
from kafka import KafkaProducer

print("Connexion à Kafka...")
producer = KafkaProducer(
    bootstrap_servers=['localhost:9094'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC_NAME = 'bike-demand-events'
FILE_PATH = 'data/hour.csv'

print(f"Lecture du fichier {FILE_PATH}...")
count = 0

with open(FILE_PATH, mode='r', encoding='utf-8') as file:
    reader = csv.DictReader(file)
    
    for row in reader:
        # --- CONVERSIONS EN ENTIERS (INT) ---
        row['instant'] = int(row['instant'])
        row['season'] = int(row['season'])
        row['yr'] = int(row['yr'])
        row['mnth'] = int(row['mnth'])
        row['hr'] = int(row['hr'])
        row['holiday'] = int(row['holiday'])
        row['weekday'] = int(row['weekday'])
        row['workingday'] = int(row['workingday'])
        row['weathersit'] = int(row['weathersit'])
        row['casual'] = int(row['casual'])
        row['registered'] = int(row['registered'])
        row['cnt'] = int(row['cnt'])
        
        # --- CONVERSIONS EN NOMBRES DÉCIMAUX (FLOAT) ---
        row['temp'] = float(row['temp'])
        row['atemp'] = float(row['atemp'])
        row['hum'] = float(row['hum'])
        row['windspeed'] = float(row['windspeed'])
        
        # Envoi dans Kafka
        producer.send(TOPIC_NAME, row)
        count += 1
        
        if count % 2000 == 0:
            print(f"{count} lignes envoyées...")
            
        # On réduit la pause pour que ça aille plus vite
        time.sleep(0.0005) 

producer.flush()
print(f" Terminé ! {count} événements envoyés dans Kafka.")