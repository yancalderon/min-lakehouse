import json
import random
from datetime import datetime, UTC
from azure.eventhub import EventHubProducerClient, EventData
from faker import Faker
import time
import logging
import gzip
import io

class TweetSimulator:
    def __init__(self, connection_str, eventhub_name):
        self.producer = EventHubProducerClient.from_connection_string(
            conn_str=connection_str,
            eventhub_name=eventhub_name
        )
        # Configurar Faker para español
        self.fake = Faker('es_ES')
        self.logger = logging.getLogger(__name__)
        
        # Hashtags relacionados a seguridad ciudadana
        self.trending_topics = [
            "#HuelgaPeru", "#ParoNacional", "#Movilizacion", "#PNP", 
            "#DelitosPeru", "#Huelga", "#Peru", "#SeguridadCiudadana",
            "#EmergenciaPeru", "#AlertaSeguridad", "#DelincuenciaPeru",
            "#PolicíaNacional", "#SerenazgoPeru", "#RoboPeru", "#AsaltoPeru"
        ]
        
        # Frases base para generar tweets más realistas
        self.frases_base = [
            "Alerta en {}: reportan {}",
            "Vecinos de {} denuncian {}",
            "Urgente: {} en la zona de {}",
            "Se registra {} en el distrito de {}",
            "{} fue reportado en {}",
            "Ciudadanos alertan sobre {} en {}",
            "PNP interviene por {} en {}",
            "Serenazgo actúa ante {} en {}",
        ]
        
        # Incidentes comunes
        self.incidentes = [
            "robo a mano armada",
            "asalto a transeúntes",
            "robo de vehículo",
            "disturbios",
            "manifestación violenta",
            "enfrentamiento entre manifestantes",
            "bloqueo de vías",
            "vandalismo",
            "robo a vivienda",
            "asalto a local comercial"
        ]
        
        # Distritos de Lima
        self.distritos = [
            "San Juan de Lurigancho",
            "Ate", 
            "San Martín de Porres",
            "Los Olivos",
            "Villa El Salvador",
            "Comas",
            "Villa María del Triunfo",
            "Miraflores",
            "San Borja",
            "La Victoria",
            "Cercado de Lima",
            "Rímac",
            "San Miguel",
            "Breña"
        ]
        
        # Usuarios simulados (con nombres en español)
        self.users = [
            {
                "id": str(i),
                "username": f"vecino_alerta_{i}",
                "followers": random.randint(100, 10000)
            } for i in range(20)
        ]

    def generate_tweet(self):
        user = random.choice(self.users)
        # Seleccionar 1-3 hashtags
        topics = random.sample(self.trending_topics, k=random.randint(1, 3))
        
        # Generar texto del tweet
        frase_base = random.choice(self.frases_base)
        distrito = random.choice(self.distritos)
        incidente = random.choice(self.incidentes)
        
        tweet_text = frase_base.format(distrito, incidente)
        
        # Agregar detalles adicionales aleatorios
        detalles = [
            "Se solicita presencia policial.",
            "Vecinos están organizados.",
            "Se reportan varios afectados.",
            "La policía ya se encuentra en el lugar.",
            "Serenazgo acude a la zona.",
            "Se recomienda tomar precauciones.",
            "Evitar la zona por el momento."
        ]
        
        tweet_text += f". {random.choice(detalles)}"
        
        # Agregar hashtags al final
        for topic in topics:
            tweet_text += f" {topic}"
            
        # Generar métricas simuladas
        likes = random.randint(0, min(user['followers'], 1000))
        retweets = random.randint(0, likes)
        replies = random.randint(0, likes // 2)
        
        # Calcular el sentiment score (tendiendo a negativo por la naturaleza de los tweets)
        sentiment_score = random.uniform(-1, -0.2)
        
        tweet = {
            "id": str(self.fake.uuid4()),
            "created_at": datetime.now(UTC).isoformat(),
            "text": tweet_text,
            "user": {
                "id": user["id"],
                "username": user["username"],
                "followers_count": user["followers"]
            },
            "metrics": {
                "likes": likes,
                "retweets": retweets,
                "replies": replies
            },
            "hashtags": topics,
            "sentiment_score": float(round(sentiment_score, 2)),
            "lang": "es",
            "location": {
                "district": distrito,
                "city": "Lima",
                "country": "Peru"
            },
            "incident_type": incidente
        }
        
        return tweet

    def compress_data(self, data):
        """Comprime los datos usando GZIP"""
        json_str = json.dumps(data, ensure_ascii=False)  # ensure_ascii=False para caracteres especiales
        out = io.BytesIO()
        
        with gzip.GzipFile(fileobj=out, mode='w') as gz:
            gz.write(json_str.encode('utf-8'))
        
        return out.getvalue()

    def send_to_eventhub(self, tweet_data):
        try:
            # Enviar datos sin comprimir
            json_str = json.dumps(tweet_data, ensure_ascii=False)
            event_data = EventData(json_str.encode('utf-8'))
            event_data.properties = {'contentType': 'application/json'}
            
            event_data_batch = self.producer.create_batch()
            event_data_batch.add(event_data)
            self.producer.send_batch(event_data_batch)
            
            self.logger.info(f"Tweet enviado: {tweet_data['text'][:100]}...")
            
        except Exception as e:
            self.logger.error(f"Error enviando a Event Hub: {str(e)}")

    def run(self, tweets_per_minute=30):
        delay = 60 / tweets_per_minute
        
        try:
            while True:
                tweet = self.generate_tweet()
                self.send_to_eventhub(tweet)
                time.sleep(delay)
        except KeyboardInterrupt:
            self.logger.info("Deteniendo el simulador...")
        finally:
            self.producer.close()

    def start(self, tweets_per_minute=30):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger.info(f"Iniciando simulador de tweets ({tweets_per_minute} tweets/minuto)...")
        self.run(tweets_per_minute)

if __name__ == "__main__":
    # Configuración
    CONNECTION_STRING = "Endpoint=sb://mia201streamdata.servicebus.windows.net/;SharedAccessKeyName=manage;SharedAccessKey=kMzAtyLPROJoOoYjVuI4neFoCs8sbtFIh+AEhA/VYQ4="
    EVENTHUB_NAME = "twitterhub"
    TWEETS_PER_MINUTE = 5
    
    simulator = TweetSimulator(CONNECTION_STRING, EVENTHUB_NAME)
    simulator.start(TWEETS_PER_MINUTE)