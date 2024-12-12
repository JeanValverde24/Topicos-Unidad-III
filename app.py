import streamlit as st
import random
import pandas as pd
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions
from couchbase.auth import PasswordAuthenticator
from confluent_kafka import Producer, Consumer

# Configuración general
st.set_page_config(
    page_title="Monitoreo de Calidad del Aire",
    page_icon="🌐",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Estilos personalizados
st.markdown("""
    <style>
        body {
            background-color: #0f0f0f;
            color: #00ff99;
        }
        .css-1d391kg, .css-18ni7ap {
            background-color: #0f0f0f;
            color: #00ff99;
        }
        button {
            border-radius: 5px;
            background: linear-gradient(to bottom, #33ccff, #ff99cc);
            border: 1px solid #33ccff;
            color: #0f0f0f;
            font-size: 16px;
        }
    </style>
""", unsafe_allow_html=True)

# Configuración de Couchbase
COUCHBASE_CLUSTER = "couchbases://cb.cj1kcvgq695ufzto.cloud.couchbase.com"
COUCHBASE_BUCKET = "TopicosAppU3"
COUCHBASE_USER = "jeanvalverde"  # Cambiar por el usuario real
COUCHBASE_PASSWORD = "Valverde24c#"  # Cambiar por la contraseña real

# Configuración de Kafka
KAFKA_BOOTSTRAP_SERVERS = "pkc-619z3.us-east1.gcp.confluent.cloud:9092"
KAFKA_API_KEY = "OGHDQZEIJ2SIKITA"  # Cambiar por la API Key
KAFKA_API_SECRET = "D3YGEWzIUka3yM5YAZn+H3dRk7SLiS/Lcl9u/xbiOmB1I8QFehaRW7Qnwyr7CYcD"  # Cambiar por el API Secret
KAFKA_TOPIC = "calidad-del-aire"

# Inicializar conexiones
def connect_to_couchbase():
    cluster = Cluster(
        COUCHBASE_CLUSTER,
        ClusterOptions(PasswordAuthenticator(COUCHBASE_USER, COUCHBASE_PASSWORD))
    )
    bucket = cluster.bucket(COUCHBASE_BUCKET)
    collection = bucket.default_collection()
    return collection

def configure_kafka_producer():
    return Producer({
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'sasl.mechanisms': 'PLAIN',
        'security.protocol': 'SASL_SSL',
        'sasl.username': KAFKA_API_KEY,
        'sasl.password': KAFKA_API_SECRET,
    })

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

collection = connect_to_couchbase()
producer = configure_kafka_producer()
consumer = configure_kafka_consumer()

# Generar datos aleatorios
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

# Interfaz
st.title("🌍 Monitoreo en Tiempo Real de Calidad del Aire")
st.subheader("Con Kafka y Couchbase")

# Botón para almacenar múltiples mensajes en Kafka
if st.button("Almacenar Mensajes en Kafka"):
    for i in range(10):
        datos_kafka = generar_datos()
        producer.produce(KAFKA_TOPIC, value=str(datos_kafka))
    producer.flush()
    st.success("Se han enviado 10 mensajes a Kafka.")

# Generar y guardar datos
if st.button("Generar y Guardar Datos de Calidad del Aire"):
    datos_couchbase = generar_datos()
    collection.upsert("ultimo_cambio", datos_couchbase)
    st.write("Datos guardados en Couchbase:")
    st.json(datos_couchbase)

# Consumir datos de Kafka
if st.button("Consumir Datos de Kafka"):
    mensaje = consumer.poll(1.0)
    if mensaje is None:
        st.warning("No hay mensajes nuevos en Kafka.")
    else:
        st.write("Mensaje consumido desde Kafka:")
        st.json(eval(mensaje.value().decode('utf-8')))

# Mostrar datos históricos
if st.button("Mostrar Datos Históricos de Couchbase"):
    documentos = []
    for i in range(1, 11):  # Cambiar el rango según el número de documentos
        try:
            doc = collection.get(f"cambio_{i}").content_as[dict]
            documentos.append(doc)
        except Exception:
            continue
    if documentos:
        df = pd.DataFrame(documentos)
        st.dataframe(df)
    else:
        st.warning("No hay datos históricos disponibles.")
