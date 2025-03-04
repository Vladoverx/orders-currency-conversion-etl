from datetime import datetime
import json
import logging
from decimal import Decimal
from airflow.decorators import dag, task
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable

@dag(
    schedule_interval="@hourly",
    start_date=datetime(2025, 3, 3),
    catchup=False,
    tags=["order_conversion"]
)
def order_conversion_dag():
    """
    DAG to use daily exchange rates for converting order amounts.
    The exchange rates are fetched (via Open Exchange Rates API) only once per day,
    and the same rates are reused by the hourly DAG runs.
    """

    @task()
    def get_exchange_rates() -> dict:
        """
        Retrieve exchange rates from Open Exchange Rates API on a daily basis.
        The exchange rates are stored in an Airflow Variable, so hourly runs reuse the
        same data if the rates were already updated today.
        """
        current_day = datetime.now().strftime("%Y-%m-%d")
        variable_name = "daily_exchange_rates"

        try:
            stored = Variable.get(variable_name, deserialize_json=True)
            if stored.get("date") == current_day:
                logging.info("Using cached exchange rates")
                return stored.get("rates", {})
        except KeyError:
            pass

        http_hook = HttpHook(method="GET", http_conn_id="open_exchange_rates")
        conn = http_hook.get_connection("open_exchange_rates")
        endpoint = f"/api/latest.json?app_id={conn.password}"
        
        response = http_hook.run(endpoint=endpoint)

        if response.status_code != 200:
            raise Exception(f"Failed to fetch exchange rates. Status code: {response.status_code}")

        data = response.json()
        usd_rates = data.get("rates", {})
        if not usd_rates or "EUR" not in usd_rates:
            raise ValueError("EUR rate is missing from the exchange rate data!")

        eur_rate = usd_rates["EUR"]
        # Normalize rates to be relative to EUR
        normalized_rates = {currency: rate / eur_rate for currency, rate in usd_rates.items()}

        rates_data = {"date": current_day, "rates": normalized_rates}
        Variable.set(variable_name, json.dumps(rates_data))

        return normalized_rates

    @task()
    def transfer_and_convert_orders(exchange_rates: dict) -> str:
        """
        Connect to the source and target PostgreSQL databases, fetch unprocessed orders,
        mark them as processed, convert the order amounts to EUR using the provided exchange rates,
        and insert the converted orders into the target database.
        """
        conversion_time = datetime.now()

        source_hook = PostgresHook(postgres_conn_id="postgres_orders")
        target_hook = PostgresHook(postgres_conn_id="postgres_processed_orders_eur")

        # Retrieve unprocessed orders
        select_sql = """
            SELECT order_id, customer_email, order_date, amount, currency
            FROM orders
            WHERE processed_at IS NULL
        """
        orders = source_hook.get_records(select_sql)

        if not orders:
            return "No new orders to process."

        # Extract order IDs and update processed_at in the source database
        order_ids = [order[0] for order in orders]
        update_sql = """
            UPDATE orders
            SET processed_at = %s
            WHERE order_id = ANY(%s::uuid[])
        """
        source_hook.run(update_sql, parameters=(conversion_time, order_ids))

        rows_to_insert = []
        for order in orders:
            order_id, email, order_date, amount, currency = order

            if currency == "EUR":
                eur_amount = amount
                rate = 1.0
            else:
                rate = exchange_rates.get(currency, 1.0)
                if rate == 1.0:
                    eur_amount = amount
                else:
                    eur_amount = round(amount / Decimal(rate), 2)

            rows_to_insert.append((
                order_id,
                email,
                order_date,
                amount,
                currency,
                eur_amount,
                rate,
                conversion_time
            ))

        target_hook.insert_rows(
            table="orders_eur",
            rows=rows_to_insert,
            target_fields=[
                "order_id",
                "customer_email",
                "order_date",
                "original_amount",
                "original_currency",
                "amount_eur",
                "exchange_rate",
                "exchange_rate_date"
            ],
            commit_every=5000
        )
        return f"Processed {len(orders)} orders."

    rates = get_exchange_rates()
    result = transfer_and_convert_orders(rates)
    
    rates >> result

order_conversion_dag_instance = order_conversion_dag()
