import json
import time
from cassandra.cluster import Cluster

CASSANDRA_HOST = "cassandra"
KEYSPACE = "agile_data_science"

for _ in range(30):
    try:
        cluster = Cluster([CASSANDRA_HOST])
        session = cluster.connect()
        break
    except Exception:
        time.sleep(5)
else:
    raise RuntimeError("No se pudo conectar a Cassandra")

session.execute(f"""
    CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
    WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
""")

session.set_keyspace(KEYSPACE)

session.execute("""
    CREATE TABLE IF NOT EXISTS origin_dest_distances (
        origin text,
        dest text,
        distance int,
        PRIMARY KEY (origin, dest)
    )
""")

session.execute("""
    CREATE TABLE IF NOT EXISTS flight_delay_predictions (
        uuid text PRIMARY KEY,
        origin text,
        dest text,
        prediction int,
        timestamp timestamp
    )
""")

print("Esquema de Cassandra inicializado correctamente")
cluster.shutdown()
