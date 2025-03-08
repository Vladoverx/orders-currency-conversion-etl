# Basic Orders Currency Conversion ETL Example
## Overview
An ETL pipeline to process generated order data with currency conversions using Apache Airflow. The pipeline automates the process of converting order amounts to the single currency Euro using current exchange rates.

### Configure Airflow Connections

Before executing the DAGs, configure the necessary Airflow connections via the Airflow UI:

1. **OpenExchangeRates Connection**
   - **Connection ID:** `open_exchange_rates`
   - **Connection Type:** HTTP
   - **Host:** `https://openexchangerates.org`
   - **Password:** Your API key

2. **PostgreSQL Connections**
   - **Source Orders Database**
     - **Connection ID:** `postgres_orders`
     - **Connection Type:** Postgres
     - **Host:** Your host
     - **Database**: postgres-1
     - **Login**: postgres-1
     - **Password:** postgres-1
     - **Port**: 5432

   - **Processed Orders Database**
     - **Connection ID:** `postgres_processed_orders_eur`
     - **Connection Type:** Postgres
     - **Host:** Your host
     - **Database**: postgres-2
     - **Login**: postgres-2
     - **Password:** postgres-2
     - **Port**: 5432
