from snakebite.client import Client

client = Client('localhost', 9870)
for x in client.ls(['/']):
    print(x)