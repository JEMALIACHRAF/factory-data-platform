"""
Test producer — envoie de vrais événements IoT vers Kafka MSK
Lance après cdk deploy FactoryStreaming-prod
"""
import json
import random
import time
from datetime import datetime, timezone

# pip install kafka-python boto3
from kafka import KafkaProducer
import boto3

# ── Récupère le bootstrap endpoint MSK ───────────────────────
def get_msk_bootstrap():
    client = boto3.client('kafka', region_name='eu-west-1')
    clusters = client.list_clusters()['ClusterInfoList']
    
    cluster = next(
        c for c in clusters 
        if c['ClusterName'] == 'factory-kafka-prod'
    )
    
    brokers = client.get_bootstrap_brokers(
        ClusterArn=cluster['ClusterArn']
    )
    return brokers['BootstrapBrokerString']


# ── Génère un événement IoT réaliste ─────────────────────────
def generate_iot_event(i: int) -> dict:
    devices = ['DEVICE-001', 'DEVICE-002', 'DEVICE-003']
    plants  = ['LYON-01', 'PARIS-01', 'BERLIN-01']
    events  = ['TEMPERATURE', 'VIBRATION', 'PRESSURE']
    
    event_name = random.choice(events)
    
    if event_name == 'TEMPERATURE':
        value = round(random.uniform(65, 115), 2)
        unit  = '°C'
        threshold = 90.0
    elif event_name == 'VIBRATION':
        value = round(random.uniform(0.5, 3.5), 2)
        unit  = 'Hz'
        threshold = 2.0
    else:
        value = round(random.uniform(2.5, 6.5), 2)
        unit  = 'bar'
        threshold = 5.0

    return {
        "event_id":        f"EVT-TEST-{i:06d}",
        "device_id":       random.choice(devices),
        "plant_id":        random.choice(plants),
        "timestamp_ms":    int(datetime.now(timezone.utc).timestamp() * 1000),
        "event_name":      event_name,
        "value_numeric":   value,
        "unit":            unit,
        "quality":         random.randint(70, 99),
        "alert_threshold": threshold,
        "firmware_version": "v2.1.0",
    }


def main():
    print(" Getting MSK bootstrap brokers...")
    bootstrap = get_msk_bootstrap()
    print(f" Bootstrap: {bootstrap}")

    producer = KafkaProducer(
        bootstrap_servers=bootstrap.split(','),
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8'),
        retries=3,
    )

    topic = 'factory-iot-events'
    n_events = 100

    print(f" Sending {n_events} events to topic '{topic}'...")

    for i in range(n_events):
        event = generate_iot_event(i)
        producer.send(
            topic,
            key=event['device_id'],
            value=event,
        )

        if i % 10 == 0:
            print(f"  Sent {i}/{n_events} events...")
        time.sleep(0.05)

    producer.flush()
    producer.close()
    print(f" Done! {n_events} events sent to Kafka topic '{topic}'")
    print("⏳ Wait 5 min for S3 Sink Connector to batch → S3 raw/")
    print("⏳ Then Glue iot_transformer will pick up the files")


if __name__ == "__main__":
    main()