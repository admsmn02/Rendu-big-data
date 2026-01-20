import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

from faker import Faker

fake=Faker()
Faker.seed(42)
random.seed(42)

def generate_client(n_clients: int, output_path: str) -> list[int]:
    """
    Generate fake client data

    Args:
        n_clients (int): Number of clients to generate.
        output_path (str): Path to save the CSV file.

    Returns:
        list[int]: List of generated client IDs.
    """

    countries = ["France", "Germany", "Italy", "Spain", "United Kingdom", "United States", "Canada", "Australia"]

    clients = []
    client_ids = []

    for i in range(1, n_clients + 1):
        date_inscription = fake.date_between(start_date='-3y', end_date='-1m')
        clients.append(
            {
                "id_client": i,
                "name": fake.name(),
                "email": fake.email(),
                "date_inscription": date_inscription.strftime("%Y-%m-%d"),
                "country": random.choice(countries)
            }
        )
        client_ids.append(i)

        Path(output_path).parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, mode='w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=["id_client", "name", "email", "date_inscription", "country"])
            writer.writeheader()
            writer.writerows(clients)

        print(f"Generated {n_clients} clients and saved to {output_path}")
    return client_ids

def generate_achats(clients_ids: list[int], avg_purchagses_per_client: int, output_path: str) -> None:
    """
    Generate fake purchase data.

    Args:
        clients_ids (list[int]): List of client IDs.
        id_achats, id_client, data_achat, montant, produit
        avg_purchagses_per_client (int): Average number of purchases per client.
        output_path (str): Path to save the CSV file.
    """

    products = ["Laptop", "Smartphone", "Tablet", "Headphones", "Smartwatch", "Camera", "Printer", "Monitor"]

    achats = []
    achat_id = 1
    for client_id in clients_ids:
        n_purchases = max(1, int(random.gauss(avg_purchagses_per_client, 2)))

        for _ in range(n_purchases):
            date_achat = fake.date_between(start_date='-1y', end_date='today')
            montant = round(random.uniform(20.0, 2000.0), 2)
            produit = random.choice(products)

            achats.append(
                {
                    "id_achat": achat_id,
                    "id_client": client_id,
                    "date_achat": date_achat.strftime("%Y-%m-%d"),
                    "montant": montant,
                    "produit": produit
                }
            )
            achat_id += 1
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=["id_achat", "id_client", "date_achat", "montant", "produit"])
        writer.writeheader()
        writer.writerows(achats)
            

if __name__ == "__main__":
    output_dir = Path(__file__).parent.parent / "data" / "sources"

    clients_ids = generate_client(
        n_clients=1500,
        output_path=str(output_dir / "clients.csv")
    )

    generate_achats(
        clients_ids=clients_ids,
        avg_purchagses_per_client=10,
        output_path=str(output_dir / "achats.csv")
    )