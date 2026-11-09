# Projet de Parcours — Pipeline IoT de supervision d'une raffinerie

Pipeline de données temps réel : des capteurs simulés (température et vibration) publient sur MQTT, une passerelle relaie les mesures vers Kafka, puis un job Spark Structured Streaming filtre les mesures, calcule des KPI et les stocke dans TimescaleDB pour les visualiser dans Grafana.

## Architecture

```
simulateur_capteurs.py ──MQTT──▶ Mosquitto ──▶ mqtt_to_kafka.py ──▶ Kafka (topic sensor-data)
                                                                          │
                                                                          ▼
                                                        Spark (spark/traitement_kpi.py)
                                                       ┌──────────────────┴──────────────────┐
                                                       ▼                                     ▼
                                           TimescaleDB : mesures_filtrees        TimescaleDB : kpi_indicateurs
                                                       └──────────────────┬──────────────────┘
                                                                          ▼
                                                                       Grafana
```

| Composant | Rôle | Port(s) hôte |
|---|---|---|
| Mosquitto (`eclipse-mosquitto:2.0`) | Broker MQTT | 1883 |
| Zookeeper + Kafka (Confluent 7.4.0) | Bus de messages | 2181, 9092 |
| Spark master + worker (`apache/spark:3.4.1`) | Traitement en streaming | 8080 (UI), 47077 (master) |
| MinIO | Stockage objet (accès S3 déjà configuré dans le job) | 9000, 9001 (console) |
| TimescaleDB (PostgreSQL 14) | Base de séries temporelles, base `iotdb` | 5432 |
| Grafana 10.2.3 | Tableaux de bord | 3000 |

Tous les services partagent le réseau Docker `iot-net`.

## Contenu du dépôt

| Fichier / dossier | Description |
|---|---|
| `docker-compose.yml` | Infrastructure complète (9 services, 4 volumes nommés) |
| `simulateur_capteurs.py` | Simule deux capteurs et publie sur MQTT toutes les 2 s |
| `mqtt_to_kafka.py` | Passerelle MQTT → Kafka (crée le topic s'il n'existe pas) |
| `spark/traitement_kpi.py` | Job Spark : lecture Kafka, filtrage, KPI, écriture TimescaleDB |
| `volumes/mqtt/mosquitto.conf` | Configuration Mosquitto (listener 1883, accès anonyme, persistance) |
| `requirements.txt` | Dépendances Python (`kafka-python`, `paho-mqtt`) |

## Données simulées

| Machine | Type | Plage simulée | Topic MQTT |
|---|---|---|---|
| `pipe-101` | `temperature` | -50 à 200 °C | `raffinerie/temp` |
| `pump-303` | `vibration` | -1 à 6 mm/s | `raffinerie/vib` |

Format des messages JSON :

```json
{
  "machine_id": "pipe-101",
  "valeur": 87.42,
  "timestamp": "2026-11-09T18:49:00Z",
  "type_capteur": "temperature"
}
```

## Traitement Spark

Le job `spark/traitement_kpi.py` lit le topic Kafka `sensor-data` (`kafka:29092`, depuis le réseau Docker) et :

1. **Filtre les mesures valides** : température entre 30 et 150 °C, vibration entre 0 et 5 mm/s. Les valeurs hors plage sont écartées.
2. **Enregistre les mesures filtrées** dans la table `mesures_filtrees` de TimescaleDB.
3. **Calcule des KPI** : moyenne par fenêtre d'une minute et par type de capteur (watermark de 30 s), écrite dans la table `kpi_indicateurs` (colonnes `timestamp`, `type_kpi`, `valeur`, `unite`).

La sauvegarde brute des données dans MinIO est désactivée pour l'instant (commentaire dans le script : le job s'arrêterait si le bucket n'existe pas).

## Prérequis

- Docker et Docker Compose
- Python 3.9 ou plus récent

## Démarrage

```bash
# 1. Cloner le dépôt
git clone https://github.com/ibralearn/Projet_de_Parcours.git
cd Projet_de_Parcours

# 2. Lancer l'infrastructure
docker compose up -d

# 3. Installer les dépendances Python
python -m venv .venv
source .venv/bin/activate        # Windows : .venv\Scripts\activate
pip install -r requirements.txt

# 4. Lancer la passerelle MQTT → Kafka (terminal 1)
python mqtt_to_kafka.py

# 5. Lancer le simulateur de capteurs (terminal 2)
python simulateur_capteurs.py
```

### Lancer le job Spark

Le dossier `spark/` est monté dans `/app` du conteneur `spark-master`. Le job a besoin des connecteurs Kafka et PostgreSQL, à fournir via `--packages` (à adapter à vos versions) :

```bash
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.postgresql:postgresql:42.6.0 \
  /app/traitement_kpi.py
```

Pour arrêter l'infrastructure : `docker compose down` (ajouter `-v` pour supprimer aussi les volumes).

## Configuration

La passerelle `mqtt_to_kafka.py` se règle par variables d'environnement :

| Variable | Valeur par défaut | Description |
|---|---|---|
| `KAFKA_BROKER` | `localhost:9092` | Adresse du broker Kafka |
| `KAFKA_TOPIC` | `sensor-data` | Topic Kafka de destination |
| `MQTT_BROKER` | `localhost` | Hôte du broker MQTT |
| `MQTT_PORT` | `1883` | Port du broker MQTT |

## Accès aux interfaces

| Service | URL | Identifiants de développement |
|---|---|---|
| Spark UI | <http://localhost:8080> | — |
| Console MinIO | <http://localhost:9001> | `minio` / `minio123` |
| Grafana | <http://localhost:3000> | à la première connexion : `admin` / `admin` (Grafana demande de le changer) |

Pour Grafana, ajouter une source de données PostgreSQL avec : hôte `timescaledb:5432`, base `iotdb`, utilisateur et mot de passe `admin`, TLS désactivé. Les tables à interroger sont `mesures_filtrees` et `kpi_indicateurs`.

## Problème connu : paho-mqtt 2.x

`requirements.txt` fixe `paho-mqtt==2.1.0`, mais `simulateur_capteurs.py` et `mqtt_to_kafka.py` créent le client avec `mqtt.Client(...)` sans préciser la version de l'API de callbacks, et `on_connect` utilise la signature de paho-mqtt 1.x. Avec paho-mqtt 2.x, cela provoque une erreur au démarrage.

Deux corrections possibles :

- fixer `paho-mqtt<2` dans `requirements.txt` ;
- ou adapter le code : `mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, ...)` pour garder les callbacks actuels.

## Sécurité

`docker-compose.yml` et `spark/traitement_kpi.py` contiennent des identifiants en clair (MinIO, TimescaleDB), et Mosquitto accepte les connexions anonymes. C'est acceptable en local pour un projet de formation ; à externaliser (fichier `.env`, secrets Docker) avant tout déploiement.

## Auteur

[ibralearn](https://github.com/ibralearn)
