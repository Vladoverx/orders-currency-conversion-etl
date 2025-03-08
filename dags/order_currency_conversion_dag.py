from datetime import datetime
import json
import logging
from decimal import Decimal
import pandas as pd
from airflow.decorators import dag, task
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from sqlalchemy import create_engine
from airflow.exceptions import AirflowException

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
        
        source_conn = source_hook.get_conn()
        target_conn = target_hook.get_conn()
        
        try:
            source_conn.autocommit = False
            target_conn.autocommit = False
            source_cursor = source_conn.cursor()
            
            # Retrieve unprocessed orders with FOR UPDATE to lock the rows
            select_sql = """
                SELECT order_id, customer_email, order_date, amount, currency
                FROM orders
                WHERE processed_at IS NULL
                LIMIT 30000
                FOR UPDATE
            """
            source_cursor.execute(select_sql)
            orders = source_cursor.fetchall()
            
            if not orders:
                source_conn.rollback()
                return "No new orders to process."
            
            orders_df = pd.DataFrame(orders, columns=[
                "order_id", "customer_email", "order_date", "amount", "currency"
            ])
            
            def convert_to_eur(row: pd.Series):
                if row["currency"] == "EUR":
                    return row["amount"], 1.0
                rate = exchange_rates.get(row["currency"], 1.0)
                eur_amount = round(row["amount"] / Decimal(str(rate)), 2)
                return eur_amount, rate
            
            orders_df[["amount_eur", "exchange_rate"]] = orders_df.apply(
                convert_to_eur, axis=1, result_type="expand"
            )

            orders_df["exchange_rate_date"] = conversion_time
            orders_df["original_amount"] = orders_df["amount"]
            orders_df["original_currency"] = orders_df["currency"]
            
            target_df = orders_df[[
                "order_id", "customer_email", "order_date", 
                "original_amount", "original_currency", 
                "amount_eur", "exchange_rate", "exchange_rate_date"
            ]]
            
            try:
                target_engine = create_engine(target_hook.get_uri())
                target_df.to_sql(
                    "orders_eur", 
                    target_engine, 
                    if_exists="append", 
                    index=False,
                    method="multi",
                    chunksize=1000
                )
            except Exception as e:
                source_conn.rollback()
                target_conn.rollback()
                raise Exception(f"Failed to insert into target database: {e}")
            
            # Then, update the source database
            try:
                order_ids = orders_df["order_id"].tolist()
                placeholders = ",".join(["%s"] * len(order_ids))
                update_sql = f"""
                    UPDATE orders
                    SET processed_at = %s
                    WHERE order_id in ({placeholders})
                """
                source_cursor.execute(update_sql, [conversion_time] + order_ids)
            except Exception as e:
                source_conn.rollback()
                target_conn.rollback()
                raise Exception(f"Failed to update source database: {e}")
            
            target_conn.commit()
            source_conn.commit()
            
            return f"Processed {len(orders)} orders."
        
        except Exception as e:
            try:
                target_conn.rollback()
            except Exception:
                pass
            
            try:
                source_conn.rollback()
            except Exception:
                pass
                
            logging.error(f"Error processing orders: {e}")
            raise AirflowException(f"Error processing orders: {e}")
        
        finally:
            source_conn.close()
            target_conn.close()

    rates = get_exchange_rates()
    result = transfer_and_convert_orders(rates)
    
    rates >> result
    
order_conversion_dag_instance = order_conversion_dag()
