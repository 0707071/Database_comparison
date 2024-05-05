from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

# Choose authentication mechanism
auth_provider = PlainTextAuthProvider(username='MyDatabase', password='Password')

# Connect to Cassandra cluster
cluster = Cluster(['127.0.0.1'], auth_provider=auth_provider)
session = cluster.connect()
