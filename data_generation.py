import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

# Fonctions pour générer les données
def generate_interactions_clients(num_records):
    data = []
    for i in range(1, num_records + 1):
        client_id = random.randint(1, 1000)
        interaction_id = i
        timestamp = datetime.now() - timedelta(days=random.randint(0, 365))
        interaction_type = random.choice(['visite_site', 'appel_service_client', 'email'])
        duration_seconds = random.randint(30, 600)
        data.append([client_id, interaction_id, timestamp, interaction_type, duration_seconds])
    return pd.DataFrame(data, columns=['client_id', 'interaction_id', 'timestamp', 'interaction_type', 'duration_seconds'])

def generate_produits_souscrits(num_records):
    data = []
    for i in range(1, num_records + 1):
        client_id = random.randint(1, 1000)
        product_id = i
        product_type = random.choice(['compte_courant', 'carte_credit', 'pret'])
        subscription_date = datetime.now() - timedelta(days=random.randint(0, 365))
        amount = round(random.uniform(500, 20000), 2)
        data.append([client_id, product_id, product_type, subscription_date, amount])
    return pd.DataFrame(data, columns=['client_id', 'product_id', 'product_type', 'subscription_date', 'amount'])

def generate_navigation_web(num_records):
    data = []
    for i in range(1, num_records + 1):
        client_id = random.randint(1, 1000)
        session_id = i
        page_id = random.randint(4000, 5000)
        timestamp = datetime.now() - timedelta(days=random.randint(0, 365))
        action = random.choice(['clic', 'telechargement'])
        duration_seconds = random.randint(10, 300)
        data.append([client_id, session_id, page_id, timestamp, action, duration_seconds])
    return pd.DataFrame(data, columns=['client_id', 'session_id', 'page_id', 'timestamp', 'action', 'duration_seconds'])

# Générer les données
interactions_clients = generate_interactions_clients(10000)
produits_souscrits = generate_produits_souscrits(5000)
navigation_web = generate_navigation_web(20000)

# Sauvegarder les données en fichiers CSV  /home/lifu237/deploiement/data
interactions_clients.to_csv('data_source/interactions_clients.csv', index=False)
produits_souscrits.to_csv('data_source/produits_souscrits.csv', index=False)
navigation_web.to_csv('data_source/navigation_web.csv', index=False)
