
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

import numpy as np
import random
import pandas as pd


auth = PlainTextAuthProvider(username='cassandra', password='cassandra')
cluster = Cluster(["0.0.0.0"], port=9042)
session = cluster.connect("store") # key_space

# session.execute("INSERT INTO employee_details (id, age, city, name) VALUES (777, 45, 'chicago', 'Tyson');")

# N = 50
# shopping_data = {
#     "userid": [i for i in range(1,N+1)],
#     "item_count": [np.random.randint(low = 1000, high = 10000, size = N)],
# }
# session.execute("INSERT INTO store.shopping_cart(userid, item_count, last_update_timestamp) VALUES ('9876', 2, toTimeStamp(now()))");

data = session.execute("SELECT * FROM shopping_cart;")

for item in data:
    print(item)