import streamlit as st
from neo4j import GraphDatabase
import pandas as pd
import os

uri = os.getenv("NEO4J_URI", "neo4j+s://2a922f63.databases.neo4j.io")
user = os.getenv("NEO4J_USER", "neo4j")
password = os.getenv("NEO4J_PASSWORD", "tk6WPquKBqWlrJE4OIs02fJArfRBYZ9CvTTsocYLCdY")


def get_driver(uri, user, password):
    try:
        driver = GraphDatabase.driver(uri, auth=(user, password))
        return driver
    except Exception as e:
        st.error(f"Error connecting to the database: {e}")
        st.stop()


driver = get_driver(uri, user, password)


def query_devices(tx):
    result = tx.run("MATCH (d:Device) RETURN d.id AS id")
    return [record["id"] for record in result]


def query_attacks(tx):
    result = tx.run("MATCH (a:Attack) RETURN a.name AS name")
    return [record["name"] for record in result]


def query_types(tx):
    result = tx.run("MATCH (t:Type) RETURN t.name AS name")
    return [record["name"] for record in result]


def query_attack_device_relationships(tx, attack):
    query = """
    MATCH (a:Attack {name: $attack})-[r:Attacked]->(d:Device)
    RETURN a.name AS attack, d.id AS device, r.ts AS ts, r.orig_p AS orig_p, r.resp_p AS resp_p, r.proto AS proto, r.conn_state AS conn_state
    """
    result = tx.run(query, attack=attack)
    return [{"attack": record["attack"], "device": record["device"], "ts": record["ts"], "orig_p": record["orig_p"],
             "resp_p": record["resp_p"], "proto": record["proto"], "conn_state": record["conn_state"]} for record in
            result]


def query_attack_type_relationships(tx):
    query = """
    MATCH (attack:Attack)-[class:CLASSIFIED_AS]->(type:Type)
    RETURN attack.name AS attack, type.name AS type
    """
    result = tx.run(query)
    return [{"attack": record["attack"], "type": record["type"]} for record in result]


def main():
    st.title("ATRIM")

    menu = ["Devices", "Attacks", "Types", "Attack-Device Relationships", "Attack-Type Relationships"]
    choice = st.sidebar.selectbox("Select Query", menu)

    with driver.session() as session:
        if choice == "Devices":
            st.subheader("All Devices")
            devices = session.read_transaction(query_devices)
            st.write(pd.DataFrame(devices, columns=["Device ID"]))

        elif choice == "Attacks":
            st.subheader("All Attacks")
            attacks = session.read_transaction(query_attacks)
            st.write(pd.DataFrame(attacks, columns=["Attack Name"]))

        elif choice == "Types":
            st.subheader("All Types")
            types = session.read_transaction(query_types)
            st.write(pd.DataFrame(types, columns=["Type Name"]))

        elif choice == "Attack-Device Relationships":
            st.subheader("Attack-Device Relationships")
            attack = st.selectbox("Select an Attack", session.read_transaction(query_attacks))
            if attack:
                relationships = session.read_transaction(query_attack_device_relationships, attack)
                st.write(pd.DataFrame(relationships,
                                      columns=["attack", "device", "ts", "orig_p", "resp_p", "proto", "conn_state"]))

        elif choice == "Attack-Type Relationships":
            st.subheader("Attack-Type Relationships")
            relationships = session.read_transaction(query_attack_type_relationships)
            st.write(pd.DataFrame(relationships, columns=["attack", "type"]))

    if st.button("Close Application"):
        st.write("Closing the application...")
        driver.close()
        os._exit(0)


if __name__ == "__main__":
    main()
