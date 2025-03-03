from datetime import datetime, timedelta
import uuid
import random
import string
import logging

from airflow.decorators import dag, task
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook


@dag(
    schedule_interval="*/10 * * * *",
    start_date=datetime(2025, 3, 3),
    catchup=False,
    tags=["order_generator"]
)
def generate_orders_dag():
    """
    DAG to fetch currency data from the OpenExchangeRates API,
    generate orders, and insert them into a PostgreSQL database
    """

    @task()
    def fetch_currencies():
        """
        Fetch a list of available currencies using the HTTP hook
        """
        http_hook = HttpHook(method="GET", http_conn_id="open_exchange_rates")
        conn = http_hook.get_connection("open_exchange_rates")
        endpoint = f"/api/currencies.json?app_id={conn.password}"
        
        response = http_hook.run(endpoint=endpoint)
        if response.status_code != 200:
            raise Exception(f"Failed to fetch currencies, status code: {response.status_code}")
        
        data = response.json()
        currencies = list(data.keys())
        logging.info("Fetched currencies: %s", currencies)
        return currencies

    @task()
    def generate_orders(currencies: list) -> list:
        """
        Generate 5000 orders using the provided list of currencies
        Returns a list of orders that will be passed to the insertion task
        """
        orders = []
        order_count = 5000
        now = datetime.now()
        for _ in range(order_count):
            days_ago = random.randint(0, 7)
            order_datetime = now - timedelta(days=days_ago)
            order_date = order_datetime.isoformat()
            order = (
                str(uuid.uuid4()),
                f"{random.choice(string.ascii_lowercase)}{random.randint(100, 9999)}@example.com",
                order_date,
                round(random.uniform(10.0, 1000.0), 2),
                random.choice(currencies),
            )
            orders.append(order)
        logging.info("Generated %d orders", len(orders))
        return orders

    @task()
    def insert_orders(orders: list):
        """
        Insert the generated orders into the "orders" table using the Postgres hook
        """
        pg_hook = PostgresHook(postgres_conn_id="postgres_orders")
        
        pg_hook.insert_rows(
            table="orders",
            rows=orders,
            target_fields=["order_id", "customer_email", "order_date", "amount", "currency"],
            commit_every=1000
        )
        logging.info("Inserted orders successfully.")
    
    currencies = fetch_currencies()
    orders = generate_orders(currencies)
    insert_orders(orders)

dag_instance = generate_orders_dag()
