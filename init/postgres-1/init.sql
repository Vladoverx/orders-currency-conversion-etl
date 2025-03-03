CREATE TABLE orders (
    order_id UUID PRIMARY KEY,
    customer_email VARCHAR(255) NOT NULL,
    order_date TIMESTAMP NOT NULL,
    amount DECIMAL(12, 2) NOT NULL,
    currency VARCHAR(3) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMP NULL
);
