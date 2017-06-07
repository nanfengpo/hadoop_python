from snakebite.client import Client
client = Client('master', 9000)
for x in client.ls(['/']):
    print(x) 
