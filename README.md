# AnonifyDB

AnonifyDB is a database anonymization engine designed for secure data transformation in multi-environment architectures. It provides a robust way to mask sensitive PII (Personally Identifiable Information) while maintaining data utility for development, testing, and analytics.

As a tool built by a Data Architect, it focuses on transactional integrity, scalability, and ease of integration into existing CI/CD pipelines.

## Core Capabilities

- **Localized Data Generation:** Context-aware anonymization focusing on DACH (DE) and US markets, ensuring realistic testing scenarios.
- **Architectural Integrity:** Built with a database-agnostic vision, utilizing abstraction layers for current PostgreSQL support and future SQL Server/MySQL integration.
- **Transactional Safety:** Full ACID compliance via atomic commits and rollbacks to prevent partial data masking.
- **Automated Schema Handling:** Dynamic generation of target masking schemas (e.g., `users_masked`) to reduce manual DBA overhead.



## Technical Specifications

- **Runtime:** Python 3.10+
- **Database Connectivity:** PostgreSQL via psycopg2
- **Transformation Engine:** Faker (Multi-locale)
- **Deployment:** Docker-ready for isolated testing

## Setup and Execution

### 1. Environment Configuration
Create a `.env` file in the root directory with the following parameters:

DB_USER=your_user
DB_PASSWORD=your_password
DB_HOST=localhost
DB_PORT=5432
DB_NAME=your_db
2. Dependency Management
It is recommended to use a virtual environment:


python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
3. Running the Engine
Execute the main transformation pipeline:


python3 main.py
Development Roadmap
[x] PostgreSQL Integration & DDL Automation

[x] Multi-locale Engine (DE/EN)

[ ] Seed-based Deterministic Anonymization

[ ] Support for complex JSONB structures

[ ] Multi-database adapters (SQL Server / MySQL)

License
Distributed under the MIT License.

Maintained by Ljubomir Colovic
Senior Data Engineer & Database Architect
https://www.linkedin.com/in/ljubomir-colovic-96369430/