import streamlit as st
import random
import pandas as pd
import json
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions
from couchbase.auth import PasswordAuthenticator
from confluent_kafka import Producer, Consumer

# Configuración general
st.set_page_config(
    page_title="Air Quality Monitoring",
    page_icon="🌍",
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
COUCHBASE_USER = "jeanvalverde"
COUCHBASE_PASSWORD = "Valverde24c#"

# Configuración de Kafka
KAFKA_BOOTSTRAP_SERVERS = "pkc-619z3.us-east1.gcp.confluent.cloud:9092"
KAFKA_API_KEY = "OGHDQZEIJ2SIKITA"
KAFKA_API_SECRET = "D3YGEWzIUka3yM5YAZn+H3dRk7SLiS/Lcl9u/xbiOmB1I8QFehaRW7Qnwyr7CYcD"
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
st.title("🌍 Real-Time Air Quality Monitoring")
st.subheader("Powered by Kafka and Couchbase")

# Botón para almacenar múltiples mensajes en Kafka
if st.button("Store Messages in Kafka"):
    for i in range(10):
        datos_kafka = generar_datos()
        producer.produce(KAFKA_TOPIC, value=json.dumps(datos_kafka))
    producer.flush()
    st.success("10 messages have been sent to Kafka.")

# Generar y guardar datos en Couchbase
if st.button("Generate and Save Air Quality Data"):
    datos_couchbase = generar_datos()
    collection.upsert("ultimo_cambio", datos_couchbase)
    st.write("Data stored in Couchbase:")
    st.json(datos_couchbase)

# Consumir datos de Kafka
if st.button("Consume Kafka Data"):
    mensaje = consumer.poll(1.0)
    if mensaje is None:
        st.warning("No new messages in Kafka.")
    elif mensaje.error():
        st.error(f"Error consuming message: {mensaje.error()}")
    else:
        st.write("Raw message from Kafka:")
        st.json(mensaje.value().decode('utf-8'))
        try:
            datos = json.loads(mensaje.value().decode('utf-8'))
            st.write("Parsed message from Kafka:")
            st.json(datos)
        except Exception as e:
            st.error(f"Error parsing message: {e}")

# Mostrar datos históricos de Couchbase
if st.button("Show Historical Data from Couchbase"):
    documentos = []
    for i in range(1, 11):
        try:
            doc = collection.get(f"cambio_{i}").content_as[dict]
            documentos.append(doc)
        except Exception:
            continue
    if documentos:
        df = pd.DataFrame(documentos)
        st.dataframe(df)
    else:
        st.warning("No historical data available.")

# Mostrar mensajes consumidos recientemente
if st.button("Show Consumed Messages History"):
    mensajes = []
    for _ in range(5):  # Consumir hasta 5 mensajes
        mensaje = consumer.poll(1.0)
        if mensaje and not mensaje.error():
            try:
                datos = json.loads(mensaje.value().decode('utf-8'))
                mensajes.append(datos)
            except Exception as e:
                st.error(f"Error parsing message: {e}")
    if mensajes:
        st.write("Messages consumed from Kafka:")
        st.json(mensajes)
    else:
        st.warning("No messages consumed.")
