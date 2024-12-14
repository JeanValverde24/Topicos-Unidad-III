import streamlit as st
import pandas as pd
import random
import logging
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions
from couchbase.auth import PasswordAuthenticator
from confluent_kafka import Producer, Consumer, KafkaException

# Habilitar registro de depuración
logging.basicConfig(level=logging.DEBUG)

# Configuración de Couchbase Capella
COUCHBASE_CLUSTER = "couchbases://cb.cj1kcvgq695ufzto.cloud.couchbase.com"
COUCHBASE_BUCKET = "TopicosAppU3"
COUCHBASE_USER = "jeanvalverde"
COUCHBASE_PASSWORD = "Valverde24c#"

# Configuración de Confluent Kafka
KAFKA_BOOTSTRAP_SERVERS = "pkc-619z3.us-east1.gcp.confluent.cloud:9092"
KAFKA_API_KEY = "OGHDQZEIJ2SIKITA"
KAFKA_API_SECRET = "D3YGEWzIUka3yM5YAZn+H3dRk7SLiS/Lcl9u/xbiOmB1I8QFehaRW7Qnwyr7CYcD"
KAFKA_TOPIC = "calidad-del-aire"

# Conexión a Couchbase
@st.cache_resource
def connect_to_couchbase():
    try:
        cluster = Cluster(
            COUCHBASE_CLUSTER,
            ClusterOptions(PasswordAuthenticator(COUCHBASE_USER, COUCHBASE_PASSWORD))
        )
        bucket = cluster.bucket(COUCHBASE_BUCKET)
        collection = bucket.default_collection()
        return collection
    except Exception as e:
        st.error(f"Error al conectar a Couchbase: {repr(e)}")
        return None

# Configuración de Kafka Producer
@st.cache_resource
def configure_kafka_producer():
    return Producer({
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'sasl.mechanisms': 'PLAIN',
        'security.protocol': 'SASL_SSL',
        'sasl.username': KAFKA_API_KEY,
        'sasl.password': KAFKA_API_SECRET,
    })

# Configuración de Kafka Consumer
@st.cache_resource
def configure_kafka_consumer():
    consumer = Consumer({
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'sasl.mechanisms': 'PLAIN',
        'security.protocol': 'SASL_SSL',
        'sasl.username': KAFKA_API_KEY,
        'sasl.password': KAFKA_API_SECRET,
        'group.id': 'streamlit-consumer',
        'auto.offset.reset': 'earliest',
    })
    consumer.subscribe([KAFKA_TOPIC])
    return consumer

# Generar datos simulados de calidad del aire
def generar_datos():
    ciudades = ["Lima", "Arequipa", "Cusco", "Trujillo", "Tacna"]
    data = {
        "Ciudad": random.choice(ciudades),
        "PM2.5": round(random.uniform(5, 150), 2),
        "PM10": round(random.uniform(10, 300), 2),
        "Humedad (%)": random.randint(30, 90),
        "Temperatura (°C)": round(random.uniform(15, 35), 1),
        "Estado": random.choice(["Bueno", "Moderado", "Peligroso"]),
    }
    return data

# Guardar datos en Couchbase
def guardar_en_couchbase(collection, key, data):
    try:
        collection.upsert(key, data)
        st.success(f"Datos guardados en Couchbase: {key}")
        logging.debug(f"Datos guardados en Couchbase: {data}")
    except Exception as e:
        st.error(f"Error al guardar en Couchbase: {repr(e)}")

# Enviar datos a Kafka
def enviar_a_kafka(producer, data):
    try:
        producer.produce(KAFKA_TOPIC, value=str(data))
        producer.flush()
        st.success("Datos enviados a Kafka con éxito.")
        logging.debug(f"Datos enviados a Kafka: {data}")
    except KafkaException as e:
        st.error(f"Error al enviar datos a Kafka: {e}")

# Consumir datos de Kafka
def consumir_de_kafka(consumer):
    try:
        msg = consumer.poll(1.0)
        if msg is None:
            return None
        if msg.error():
            raise KafkaException(msg.error())
        return eval(msg.value().decode('utf-8'))  # Convierte el mensaje a un diccionario
    except KafkaException as e:
        st.error(f"Error al consumir mensajes de Kafka: {e}")
        return None

# Almacenar datos ficticios en Couchbase
def almacenar_datos_ficticios(collection, num_datos=10):
    try:
        for i in range(1, num_datos + 1):
            key = f"cambio_{i}"
            data = generar_datos()
            collection.upsert(key, data)
        st.success(f"Se almacenaron {num_datos} datos ficticios en Couchbase.")
    except Exception as e:
        st.error(f"Error al almacenar datos ficticios: {repr(e)}")

# Inicializar conexiones
collection = connect_to_couchbase()
producer = configure_kafka_producer()
consumer = configure_kafka_consumer()

# Interfaz de Streamlit
st.title("Plataforma de Monitoreo de Calidad del Aire")

if collection and producer and consumer:
    # Generar y guardar datos en Couchbase y Kafka (diferentes datos)
    if st.button("Generar y Guardar Datos de Calidad del Aire"):
        datos_couchbase = generar_datos()
        guardar_en_couchbase(collection, "ultimo_cambio", datos_couchbase)

        datos_kafka = generar_datos()  # Generar un conjunto diferente de datos
        enviar_a_kafka(producer, datos_kafka)

    # Almacenar datos ficticios en Couchbase
    if st.button("Almacenar Datos Ficticios en Couchbase"):
        almacenar_datos_ficticios(collection, num_datos=10)

    # Mostrar último cambio guardado en Couchbase
    st.subheader("Últimos Datos de Calidad del Aire (Couchbase)")
    try:
        ultimo_datos = collection.get("ultimo_cambio").content_as[dict]
        if ultimo_datos:
            st.json(ultimo_datos)
        else:
            st.info("No hay datos recientes disponibles en Couchbase.")
    except Exception as e:
        st.error(f"Error al recuperar datos recientes de Couchbase: {repr(e)}")

    # Consumir mensajes de Kafka
    st.subheader("Mensajes en Tiempo Real desde Kafka")
    if st.button("Consumir Datos de Kafka"):
        mensaje = consumir_de_kafka(consumer)
        if mensaje:
            st.json(mensaje)
        else:
            st.info("No hay mensajes nuevos en Kafka.")

    # Mostrar histórico de cambios en Couchbase
    st.subheader("Histórico de Cambios (Couchbase)")
    if st.button("Mostrar Histórico"):
        cambios = []
        for i in range(1, 20):
            key = f"cambio_{i}"
            try:
                dato = collection.get(key).content_as[dict]
                cambios.append(dato)
            except Exception:
                pass
        if cambios:
            df = pd.DataFrame(cambios)
            st.dataframe(df)
            st.line_chart(df["PM2.5"].astype(float))
            st.bar_chart(df[["Humedad (%)", "Temperatura (°C)"]])
        else:
            st.info("No hay datos históricos disponibles en Couchbase.")
