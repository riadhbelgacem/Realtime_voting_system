import time
import os
import random
import psycopg2
import simplejson as json
from confluent_kafka import Consumer, KafkaError, KafkaException, SerializingProducer
from datetime import datetime
import requests
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

config = {
    'bootstrap.servers': os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),
}
consumer = Consumer(config | {
    'group.id': 'voting-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
})

producer = SerializingProducer(config)
if __name__ == "__main__":
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        database=os.getenv("POSTGRES_DB", "voting"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "postgres"),
        port=os.getenv("POSTGRES_PORT", "5432")
    )
    cur = conn.cursor()
    
    candidates_query = "SELECT row_to_json(col) FROM (SELECT * FROM candidates) col"
    cur.execute(candidates_query)
    candidates = cur.fetchall()
    candidates = [candidate[0] for candidate in candidates] 
    print(candidates)
    if len(candidates) == 0:
        raise Exception("No candidates found in the database.")
    else:
        print(f"Found {len(candidates)} candidates in the database.")
    consumer.subscribe(['voters_topic'])
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            elif msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    raise KafkaException(msg.error())

            else:
                voter = json.loads(msg.value().decode('utf-8'))
                chosen_candidate = random.choice(candidates)
                vote = voter | chosen_candidate | {"voting_time":datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),"vote":1}
                try:
                    print('User {} is voting for candidate {}'.format(voter['voter_id'], vote['candidate_id']))
                    cur.execute(
                        "INSERT INTO votes (voter_id, candidate_id, voting_time) VALUES (%s, %s, %s)",
                        (vote['voter_id'], vote['candidate_id'], vote['voting_time'])
                    )
                    conn.commit()
                    producer.produce('votes_topic', vote['voter_id'], json.dumps(vote), on_delivery=delivery_report)
                    producer.poll(0) 
            
                
                except Exception as e:
                    print(f"Error processing vote: {e}")
                    conn.rollback() 
            time.sleep(0.2)

    except Exception as e:
        print(e)


    
    cur.close()
    conn.close()