-- 1. Kreiranje šeme
CREATE SCHEMA IF NOT EXISTS ecommerce;

-- 2. Brisanje tabela ako postoje (obrnutim redosledom zbog FK)
DROP TABLE IF EXISTS ecommerce.order_items;
DROP TABLE IF EXISTS ecommerce.orders;
DROP TABLE IF EXISTS ecommerce.products;
DROP TABLE IF EXISTS ecommerce.customers;

-- 3. Kreiranje tabela sa PK i FK relacijama

-- Roditeljska tabela 1: Customers
CREATE TABLE ecommerce.customers (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100) UNIQUE,
    city VARCHAR(50),
    registration_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Roditeljska tabela 2: Products
CREATE TABLE ecommerce.products (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(100),
    category VARCHAR(50),
    price DECIMAL(10, 2)
);

-- Dete tabela 1: Orders (Zavisi od Customers)
CREATE TABLE ecommerce.orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES ecommerce.customers(customer_id),
    order_date DATE,
    status VARCHAR(20)
);

-- Dete tabela 2: Order Items (Zavisi od Orders i Products)
-- Ovo je tabela 3. nivoa zavisnosti
CREATE TABLE ecommerce.order_items (
    item_id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES ecommerce.orders(order_id),
    product_id INTEGER REFERENCES ecommerce.products(product_id),
    quantity INTEGER,
    unit_price DECIMAL(10, 2)
);

-- 4. Insert testnih podataka
INSERT INTO ecommerce.customers (first_name, last_name, email, city) VALUES
('Ljubomir', 'Colovic', 'ljubomir@example.com', 'Uzice'),
('Hans', 'Müller', 'hans.m@dach-region.de', 'Berlin'),
('John', 'Doe', 'john.doe@us-market.com', 'New York');

INSERT INTO ecommerce.products (product_name, category, price) VALUES
('Laptop Pro 15', 'Electronics', 1500.00),
('Mechanical Keyboard', 'Accessories', 120.00),
('Office Chair', 'Furniture', 250.00);

INSERT INTO ecommerce.orders (customer_id, order_date, status) VALUES
(1, '2026-03-01', 'Completed'),
(2, '2026-03-05', 'Processing'),
(3, '2026-03-10', 'Shipped');

INSERT INTO ecommerce.order_items (order_id, product_id, quantity, unit_price) VALUES
(1, 1, 1, 1500.00),
(1, 2, 1, 120.00),
(2, 3, 2, 250.00),
(3, 1, 1, 1500.00);