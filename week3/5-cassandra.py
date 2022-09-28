
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

import numpy as np
import pandas as pd
import random


auth = PlainTextAuthProvider(username='cassandra', password='cassandra')
cluster = Cluster(["0.0.0.0"], port=9042)
session = cluster.connect("store") # key_space

# session.execute("INSERT INTO employee_details (id, age, city, name) VALUES (777, 45, 'chicago', 'Tyson');")

N = 100
shopping_data = {
    "userid": list(np.random.randint(low = 9000, high = 10000, size = N)),
    "item_count": list(np.random.randint(low = 1000, high = 10000, size = N)),
}

# pass shopping_data dict to shopping_data table of key_space store

df_shopping_data = pd.DataFrame(shopping_data)

# print(df_shopping_data.head())

for item in df_shopping_data.values.tolist():
    session.execute(f"INSERT INTO store.shopping_cart(userid, item_count, last_update_timestamp) VALUES ('{item[0]}', {item[1]}, toTimeStamp(now()))");


# data = session.execute("SELECT * FROM shopping_cart;")

data = session.execute("SELECT * FROM shopping_cart LIMIT 50")

for item in data:
    print(item)