CREATE TABLE orders_eur (
    order_id UUID PRIMARY KEY,
    customer_email VARCHAR(255) NOT NULL,
    order_date TIMESTAMP NOT NULL,
    original_amount DECIMAL(12, 2) NOT NULL,
    original_currency VARCHAR(3) NOT NULL,
    amount_eur DECIMAL(12, 2) NOT NULL,
    exchange_rate DECIMAL(16, 6) NOT NULL,
    exchange_rate_date TIMESTAMP NOT NULL,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);