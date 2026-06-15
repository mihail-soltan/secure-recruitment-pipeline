from confluent_kafka import Producer

conf_nesecurizat = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'rogue-attacker-script'
}

def acked(err, msg):
    if err is not None:
        print(f"\n[EROARE DE SECURITATE] Interceptare blocata de broker: {err}")
    else:
        print(f"\n[SUCCES] Mesaj trimis.")

print("initiere conexiune nesecurizata catre Kafka...")

try:
    producer = Producer(conf_nesecurizat)
    producer.produce('cv_uri_brute', key="hacked_id", value="date false", callback=acked)
    producer.flush(timeout=5.0)
    
except Exception as e:
    print(f"\n[EROARE LOCALA] Nu m-am putut conecta: {e}")