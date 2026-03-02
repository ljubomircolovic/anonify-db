-- Create a new schema
CREATE SCHEMA IF NOT EXISTS person;

-- Create a table with sample PII data
CREATE TABLE person.employees (
    id SERIAL PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100),
    salary NUMERIC,
    phone_number VARCHAR(20)
);

-- Insert some "human" data for Llama to analyze
INSERT INTO person.employees (first_name, last_name, email, salary, phone_number) VALUES
('Ljubomir', 'Colovic', 'ljupcecar@gmail.com', 95000, '+381-64-1234567'),
('Ken', 'Sanchez', 'ken0@adventure-works.com', 125000, '1 (11) 500 555-0110'),
('Terri', 'Duffy', 'terri.duffy@aw.com', 72000, '1 (11) 500 555-0111'),
('Roberto', 'Tamburello', 'roberto.t@provider.net', 43000, '1 (11) 500 555-0112');