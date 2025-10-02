import os
import time
import random
import requests
import psycopg2
import json
import simplejson
from confluent_kafka import SerializingProducer

BASE_URL = 'https://randomuser.me/api/?nat=gb'
PARTIES = ["Management Party", "Savior Party", "Tech Republic Party"]
random.seed(42)


def generate_voter_data():
    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = requests.get(BASE_URL, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if not data.get('results') or len(data['results']) == 0:
                    if attempt < max_retries - 1:
                        time.sleep(2 ** attempt)  # Exponential backoff: 1s, 2s, 4s
                        continue
                    return None
                
                user_data = data['results'][0]
                return {
                    "voter_id": user_data['login']['uuid'],
                    "voter_name": f"{user_data['name']['first']} {user_data['name']['last']}",
                    "date_of_birth": user_data['dob']['date'],
                    "gender": user_data.get('gender', 'N/A'),
                    "nationality": user_data.get('nat', 'N/A'),
                    "registration_number": user_data['login']['username'],
                    "address": {
                        "street": f"{user_data['location']['street'].get('number', '')} {user_data['location']['street'].get('name', '')}".strip(),
                        "city": user_data['location'].get('city', ''),
                        "state": user_data['location'].get('state', ''),
                        "country": user_data['location'].get('country', ''),
                        "postcode": str(user_data['location'].get('postcode', ''))
                    },
                    "email": user_data.get('email', ''),
                    "phone_number": user_data.get('phone', ''),
                    "cell_number": user_data.get('cell', ''),
                    "picture": user_data['picture'].get('large', ''),
                    "registered_age": user_data.get('registered', {}).get('age', 0)
                }
            else:
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                    continue
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
                continue
    return None


def generate_candidate_data(candidate_number, total_parties):
    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = requests.get(BASE_URL + '&gender=' + ('female' if candidate_number % 2 == 1 else 'male'), timeout=10)
            if response.status_code == 200:
                data = response.json()
                if not data.get('results') or len(data['results']) == 0:
                    if attempt < max_retries - 1:
                        time.sleep(2 ** attempt)
                        continue
                    return None
                
                user_data = data['results'][0]
                return {
                    "candidate_id": user_data['login']['uuid'],
                    "candidate_name": f"{user_data['name']['first']} {user_data['name']['last']}",
                    "party_affiliation": PARTIES[candidate_number % total_parties],
                    "biography": "A brief bio of the candidate.",
                    "campaign_platform": "Key campaign promises or platform.",
                    "photo_url": user_data['picture'].get('large', '')
                }
            else:
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                    continue
        except Exception:
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
                continue
    return None


def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')


voters_topic = 'voters_topic'
candidates_topic = 'candidates_topic'


def drop_all_tables(conn, cur):
    print("Dropping existing tables...")
    cur.execute("DROP TABLE IF EXISTS votes CASCADE")
    cur.execute("DROP TABLE IF EXISTS voters CASCADE")
    cur.execute("DROP TABLE IF EXISTS candidates CASCADE")
    conn.commit()
    print("All tables dropped successfully")


def create_tables(conn, cur):
    print("Creating tables...")
    cur.execute("""
        CREATE TABLE candidates (
            candidate_id VARCHAR(255) PRIMARY KEY,
            candidate_name VARCHAR(255),
            party_affiliation VARCHAR(255),
            biography TEXT,
            campaign_platform TEXT,
            photo_url TEXT
        )
    """)
    
    cur.execute("""
        CREATE TABLE voters (
            voter_id VARCHAR(255) PRIMARY KEY,
            voter_name VARCHAR(255),
            date_of_birth VARCHAR(255),
            gender VARCHAR(255),
            nationality VARCHAR(255),
            registration_number VARCHAR(255),
            address_street VARCHAR(255),
            address_city VARCHAR(255),
            address_state VARCHAR(255),
            address_country VARCHAR(255),
            address_postcode VARCHAR(255),
            email VARCHAR(255),
            phone_number VARCHAR(255),
            cell_number VARCHAR(255),
            picture TEXT,
            registered_age INTEGER
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS votes (
            voter_id VARCHAR(255) UNIQUE,
            candidate_id VARCHAR(255),
            voting_time TIMESTAMP,
            vote int DEFAULT 1,
            PRIMARY KEY (voter_id, candidate_id)
        )
    """)
    conn.commit()
    print("Tables created successfully")


def insert_voters(conn, cur, voter):
    try:
        cur.execute("""
            INSERT INTO voters (
                voter_id, voter_name, date_of_birth, gender, nationality, 
                registration_number, address_street, address_city, address_state, 
                address_country, address_postcode, email, phone_number, 
                cell_number, picture, registered_age
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
            (voter["voter_id"], voter['voter_name'], voter['date_of_birth'], 
             voter['gender'], voter['nationality'], voter['registration_number'], 
             voter['address']['street'], voter['address']['city'], voter['address']['state'], 
             voter['address']['country'], voter['address']['postcode'], voter['email'], 
             voter['phone_number'], voter['cell_number'], voter['picture'], 
             voter['registered_age'])
        )
        conn.commit()
        print(f"Inserted voter: {voter['voter_name']}")
    except psycopg2.IntegrityError:
        print(f"Voter {voter['voter_id']} already exists. Skipping.")
        conn.rollback()
    except Exception as e:
        print(f"Error inserting voter: {e}")
        conn.rollback()


if __name__ == "__main__":
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            dbname=os.getenv("POSTGRES_DB", "voting"),
            user=os.getenv("POSTGRES_USER", "postgres"),
            password=os.getenv("POSTGRES_PASSWORD", "postgres"),
            port=os.getenv("POSTGRES_PORT", "5432")
        )
        cur = conn.cursor()

        kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
        print(f"Connecting to Kafka at: {kafka_servers}")
        producer = SerializingProducer({
            'bootstrap.servers': kafka_servers
        })

        drop_all_tables(conn, cur)
        create_tables(conn, cur)

        print("\nInserting candidates...")
        for i in range(3):
            candidate = generate_candidate_data(i, 3)
            if candidate:
                print(f"Candidate {i}: {candidate}")
                cur.execute("""
                    INSERT INTO candidates (
                        candidate_id, candidate_name, party_affiliation, 
                        biography, campaign_platform, photo_url
                    )
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (
                    candidate['candidate_id'], candidate['candidate_name'], 
                    candidate['party_affiliation'], candidate['biography'],
                    candidate['campaign_platform'], candidate['photo_url']
                ))
                conn.commit()
        print("All candidates inserted successfully\n")

        print("Inserting voters and sending to Kafka...")
        successful = 0
        failed = 0
        
        for i in range(1000):
            voter_data = generate_voter_data()
            if voter_data is None:
                print(f"Skipping voter {i} - failed after retries")
                failed += 1
                continue
            
            insert_voters(conn, cur, voter_data)
            producer.produce(
                voters_topic,
                key=voter_data["voter_id"],
                value=json.dumps(voter_data),
                on_delivery=delivery_report
            )
            print(f'Produced voter {i}, ID: {voter_data["voter_id"]}')
            producer.flush()
            successful += 1
            time.sleep(0.5)

        print(f"\nCompleted! Success: {successful}, Failed: {failed}")
        
 
