from datetime import datetime, timedelta
import uuid
import random
import string
import logging
import json
import pandas as pd
from airflow.decorators import dag, task
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable

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
    def fetch_currencies() -> list:
        """
        Fetch a list of available currencies using the HTTP hook
        """
        current_day = datetime.now().strftime("%Y-%m-%d")
        variable_name = "daily_currencies"
        try:
            stored = Variable.get(variable_name, deserialize_json=True)
            if stored.get("date") == current_day:
                logging.info("Using cached currencies")
                return stored.get("currencies", {})
        except KeyError:
            pass
        
        http_hook = HttpHook(method="GET", http_conn_id="open_exchange_rates")
        conn = http_hook.get_connection("open_exchange_rates")
        endpoint = f"/api/currencies.json?app_id={conn.password}"
        
        response = http_hook.run(endpoint=endpoint)
        if response.status_code != 200:
            raise Exception(f"Failed to fetch currencies, status code: {response.status_code}")
        
        data = response.json()
        currencies = list(data.keys())
        logging.info("Fetched currencies: %s", currencies)
        
        rates_data = {"date": current_day, "currencies": currencies}
        Variable.set(variable_name, json.dumps(rates_data))

        return currencies

    @task()
    def generate_orders(currencies: list) -> pd.DataFrame:
        """
        Generate 5000 orders using the provided list of currencies
        """
        order_count = 5000
        now = datetime.now()
        
        orders = pd.DataFrame({
            'order_id': [str(uuid.uuid4()) for _ in range(order_count)],
            'customer_email': [f"{random.choice(string.ascii_lowercase)}{random.randint(100, 9999)}@example.com" for _ in range(order_count)],
            'order_date': [(now - timedelta(days=random.randint(0, 7))).isoformat() for _ in range(order_count)],
            'amount': [round(random.uniform(10.0, 1000.0), 2) for _ in range(order_count)],
            'currency': [random.choice(currencies) for _ in range(order_count)]
        })
        
        logging.info("Generated %d orders", len(orders))
        return orders

    @task()
    def insert_orders(orders_df: pd.DataFrame):
        """
        Insert the generated orders into the "orders" table using the Postgres hook
        """
        pg_hook = PostgresHook(postgres_conn_id="postgres_orders")
        
        orders_records = orders_df.to_records(index=False)
        orders_list = list(orders_records)
        
        pg_hook.insert_rows(
            table="orders",
            rows=orders_list,
            target_fields=["order_id", "customer_email", "order_date", "amount", "currency"],
            commit_every=1000
        )
        logging.info("Inserted orders successfully.")
    
    currencies = fetch_currencies()
    orders_df = generate_orders(currencies)
    insert_orders(orders_df)

dag_instance = generate_orders_dag()
