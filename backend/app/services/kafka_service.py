from confluent_kafka import Producer
import json
import os
import logging
logger = logging.getLogger("uvicorn.error")
logger.setLevel(logging.DEBUG)
class KafkaProducerService:
    def __init__(self):
        current_dir = os.path.dirname(os.path.abspath(__file__))
        default_ca_path = os.path.abspath(os.path.join(current_dir, "../../../secrets/ca-cert.pem"))        
        ca_location = os.getenv("SSL_CA_LOCATION", default_ca_path)

        if not os.path.exists(ca_location):
            raise FileNotFoundError(f"Certificatul SSL nu a fost gasit la: {ca_location}")
        
        conf = {
            'bootstrap.servers': os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            'client.id': 'fastapi-producer',

            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'SCRAM-SHA-256',
            'sasl.username': os.getenv("KAFKA_USER", "app_user"),
            'sasl.password': os.getenv("KAFKA_PASSWORD", "app_password"),
            
            'ssl.ca.location': ca_location,
            'message.max.bytes': 10485760
        }

        self.producer = Producer(conf)

    def delivery_report(self, err, msg):
        if err is not None:
            print(f"Message delivery failed: {err}")
        else:
            print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    def send_cv_to_pipeline(self, app_id: int, candidate_email: str, file_content: bytes):
        payload = {
            "app_id": app_id,
            "email": candidate_email,
            "file_content": file_content
        }
        json_payload = json.dumps(payload)
        payload_size_mb = len(json_payload.encode('utf-8')) / (1024 * 1024)
        logger.info(f"DEBUG: Final Kafka payload size is {payload_size_mb:.2f} MB")
        self.producer.produce(
            'cv_uri_brute', 
            key=str(app_id), 
            value=json.dumps(payload), 
            callback=self.delivery_report
        )
        self.producer.flush()

kafka_service = KafkaProducerService()