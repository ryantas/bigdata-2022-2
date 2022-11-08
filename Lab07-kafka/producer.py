from confluent_kafka import Producer
import pandas as pd
import socket
import json
import time

# read dataset "Solargis_min15_Almeria_spain.csv"
# sample_size = 100

class App():
    def __init__(self) -> None:
        self.topic = "test"
        self.conf_producer = {
            'bootstrap.servers': 'localhost:9092, localhost:9093, localhost:9094',        
            'client.id': socket.gethostname(),
        }
        self.producer = Producer(self.conf_producer)

    
    def produce(self, data):
        self.producer.produce(self.topic, key=None, value=data)
        self.producer.flush()


def produce_dataframe(df):
    for row in df.values:
        value = {"Date": row[0], "GTI": row[2], "Temperature": row[8]}
        print(value)
        app.produce(json.dumps(value))
        time.sleep(.25)

def produce_dataframe_by_chunks(df):
    for chunk_df in df:
        produce_dataframe(chunk_df)


app = App()

# df = pd.read_csv('../datasets/csv/Solargis_min15_Almeria_Spain.csv')

df100 = pd.read_csv('../datasets/csv/Solargis_min15_Almeria_Spain.csv', chunksize=100)

produce_dataframe_by_chunks(df100)

# app.produce('data-from-produce python api')