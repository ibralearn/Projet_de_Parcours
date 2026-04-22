import os
import sys
import signal
import json
import paho.mqtt.client as mqtt
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

# --- Configuration ---
# Utiliser des variables d'environnement avec des valeurs par défaut pour plus de flexibilité
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
MQTT_BROKER = os.getenv("MQTT_BROKER", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", 1883))
MQTT_TOPICS = [("raffinerie/temp", 0), ("raffinerie/vib", 0)]  # Liste de tuples (topic, qos)
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sensor-data")

# --- Initialisation des clients (variables globales pour le graceful shutdown) ---
producer = None
mqtt_client = None


def create_kafka_topic():
    """Crée le topic Kafka s'il n'existe pas."""
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BROKER)
        topic_list = [NewTopic(name=KAFKA_TOPIC, num_partitions=1, replication_factor=1)]
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
        print(f"Topic '{KAFKA_TOPIC}' créé avec succès.")
    except TopicAlreadyExistsError:
        print(f"Le topic '{KAFKA_TOPIC}' existe déjà.")
    except Exception as e:
        print(f"Erreur critique lors de la création du topic Kafka : {e}")
        sys.exit(1)


def get_kafka_producer():
    """Initialise et retourne un producteur Kafka optimisé."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        # Améliore la performance en groupant les messages (batching)
        linger_ms=100,  # Attend jusqu'à 100ms pour envoyer un batch
        batch_size=16384  # Taille du batch en bytes (16KB)
    )


# --- Callbacks MQTT ---
def on_connect(client, userdata, flags, rc):
    """Callback exécuté lors de la connexion au broker MQTT."""
    if rc == 0:
        print(f"Connecté au broker MQTT à {MQTT_BROKER}:{MQTT_PORT}")
        client.subscribe(MQTT_TOPICS)
        print(f"Abonné aux topics: {[topic[0] for topic in MQTT_TOPICS]}")
    else:
        print(f"Échec de la connexion MQTT, code de retour: {rc}\n")


def on_message(client, userdata, msg):
    """Callback exécuté à la réception d'un message MQTT."""
    print(f"Message reçu sur le topic {msg.topic}, transfert vers Kafka...")
    try:
        data = json.loads(msg.payload.decode())
        # Envoi asynchrone à Kafka. Le flush est géré par le producer pour la performance.
        producer.send(KAFKA_TOPIC, data)
    except json.JSONDecodeError as e:
        print(f"Erreur de décodage JSON : {e} - Message: {msg.payload.decode()}")
    except Exception as e:
        print(f"Erreur lors de l'envoi vers Kafka : {e}")


def graceful_shutdown(signum, frame):
    """Gère l'arrêt propre de l'application pour éviter la perte de données."""
    print("\nArrêt du script en cours...")
    if mqtt_client and mqtt_client.is_connected():
        mqtt_client.loop_stop()
        mqtt_client.disconnect()
        print("Client MQTT déconnecté.")
    if producer:
        print("Envoi des derniers messages Kafka en attente...")
        producer.flush()  # S'assurer que tous les messages en attente sont envoyés
        producer.close()
        print("Producteur Kafka fermé.")
    sys.exit(0)


# --- Point d'entrée principal ---
if __name__ == "__main__":
    # Gérer les signaux d'arrêt (ex: Ctrl+C) pour un arrêt propre
    signal.signal(signal.SIGINT, graceful_shutdown)
    signal.signal(signal.SIGTERM, graceful_shutdown)

    create_kafka_topic()
    producer = get_kafka_producer()

    mqtt_client = mqtt.Client(client_id="mqtt-kafka-bridge-01")
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message

    try:
        mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
        mqtt_client.loop_forever()
    except Exception as e:
        print(f"Erreur de connexion MQTT : {e}")
        graceful_shutdown(None, None)
