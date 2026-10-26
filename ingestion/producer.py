import csv
import json
import time
from kafka import KafkaProducer

print("Connexion à Kafka...")
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC_NAME = 'bike-demand-events'
FILE_PATH = 'data/hour.csv'

print(f"Lecture du fichier {FILE_PATH}...")
count = 0

with open(FILE_PATH, mode='r', encoding='utf-8') as file:
    reader = csv.DictReader(file)
    
    for row in reader:
        row['hr'] = int(row['hr'])
        row['temp'] = float(row['temp'])
        row['cnt'] = int(row['cnt'])
        
        producer.send(TOPIC_NAME, row)
        count += 1
        
        if count % 1000 == 0:
            print(f"{count} lignes envoyées...")
            if count >= 5000: # On s'arrête à 5000 pour le test
                break
            
        time.sleep(0.001) # Pause très courte pour simuler le temps réel

producer.flush()
print(f"✅ Terminé ! {count} événements envoyés dans Kafka.")