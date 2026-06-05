import json 

from cassandra.cluster import Cluster 

 

cluster = Cluster(['127.0.0.1']) 

session = cluster.connect('agile_data_science') 

 

with open('data/origin_dest_distances.jsonl') as f: 

    for line in f: 

        record = json.loads(line) 

 

        origin = record['Origin'] 

        dest = record['Dest'] 

        distance = int(record['Distance']) 

 

        session.execute( 

            """ 

            INSERT INTO origin_dest_distances (origin, dest, distance) 

            VALUES (%s, %s, %s) 

            """, 

            (origin, dest, distance) 

        ) 

 

print("Datos importados correctamente") 
