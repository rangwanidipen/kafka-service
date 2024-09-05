from kafka import KafkaProducer
import json
import logging

class KafkaMessageProducer:
    def __init__(self, bootstrap_servers: str):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),  # Manually serialize the value
        )
        logging.info("Kafka Producer initialized successfully.")

    def send_message(self, topic: str, value: dict):
        try:
            self.producer.send(topic, value=value)
            self.producer.flush()
            logging.info(f"Message sent to topic {topic}: {value}")
        except Exception as e:
            logging.error(f"Failed to send message to topic {topic}: {e}")
            raise e

    def close(self):
        self.producer.close()
        logging.info("Kafka Producer closed.")
