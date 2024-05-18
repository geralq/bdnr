from neo4j import GraphDatabase
import pandas as pd


def create_devices(tx, df):
    devices_id = df['id.orig_h'].unique()
    for device_id in devices_id:
        tx.run("""
            MERGE (d:Device {id: $device_id})
        """, device_id=device_id)


def create_attacks(tx, df):
    attacks = df['detailed-label'].drop_duplicates()
    for attack in attacks:
        tx.run("""
            MERGE (a:Attack {name: $attack})
        """, attack=attack)


def create_types_of_attacks(tx, df):
    types = df['label'].drop_duplicates()
    for type in types:
        tx.run("""
            MERGE (t:Type {name: $type})
        """, type=type)


def create_attack_type_relationship(tx, df):
    unique_combinations = df[['detailed-label', 'label']].drop_duplicates()
    for _, row in unique_combinations.iterrows():
        tx.run("""
            MATCH (a:Attack {name: $attack})
            MATCH (t:Type {name: $type})
            MERGE (a)-[:CLASSIFIED_AS]->(t)
        """, attack=row['detailed-label'], type=row['label'])


def create_attack_device_relationship(tx, df):
    unique_combinations = df[['id.orig_h', 'detailed-label']].drop_duplicates()

    attacks_example = []

    for _, row in unique_combinations.iterrows():
        attacks_example.append(_)

    filtered_df = df.loc[attacks_example]

    for _, detail_row in filtered_df.iterrows():
        tx.run("""
                    MATCH (a:Attack {name: $attack})
                    MATCH (d:Device {id: $device_id})
                    MERGE (a)-[:Attacked {ts: $ts, orig_p: $orig_p, resp_p: $resp_p, proto: $proto, conn_state: $conn_state}]->(d)
                """,
               attack=detail_row['detailed-label'],
               device_id=detail_row['id.orig_h'],
               ts=detail_row['ts'],
               orig_p=detail_row['id.orig_p'],
               resp_p=detail_row['id.resp_p'],
               proto=detail_row['proto'],
               conn_state=detail_row['conn_state']
               )


uri = "neo4j+s://2a922f63.databases.neo4j.io"  # URI de la base de datos Neo4j
user = "neo4j"  # Nombre de usuario de Neo4j
password = "tk6WPquKBqWlrJE4OIs02fJArfRBYZ9CvTTsocYLCdY"

driver = GraphDatabase.driver(uri, auth=(user, password))

df = pd.read_csv('./data4.csv')

with driver.session() as session:
    session.write_transaction(create_devices, df)

    # Crear ataque
    session.write_transaction(create_attacks, df)

    # Crear tipo de ataque
    session.write_transaction(create_types_of_attacks, df)

    # Crear relación entre dispositivo y ataque
    session.write_transaction(create_attack_type_relationship, df)

    # Crear relación entre ataque y tipo de ataque
    session.write_transaction(create_attack_device_relationship, df)
driver.close()
