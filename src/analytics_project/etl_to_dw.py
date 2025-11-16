import pathlib
import sqlite3
import sys

import pandas as pd

# For local imports, temporarily add project root to sys.path
PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

# Constants - adjust these paths to match your project structure
DW_DIR = pathlib.Path("data").joinpath("dw")
DB_PATH = DW_DIR.joinpath("smart_sales.db")  # Your database name
PREPARED_DATA_DIR = PROJECT_ROOT.parent.joinpath("src", "data", "prepared")


def create_schema(cursor: sqlite3.Cursor) -> None:
    """Create tables in the data warehouse if they don't exist."""
    cursor.executescript("""
        CREATE TABLE IF NOT EXISTS customers (
            customer_id TEXT PRIMARY KEY,
            name TEXT,
            region TEXT,
            join_date TEXT,
            age INTEGER,
            subscription_status TEXT
        );

        CREATE TABLE IF NOT EXISTS products (
            product_id TEXT PRIMARY KEY,
            product_name TEXT,
            category TEXT,
            unit_price REAL,
            manufacture_year INTEGER,
            availability_status TEXT
        );

        CREATE TABLE IF NOT EXISTS sales (
            sale_id INTEGER PRIMARY KEY,
            date TEXT,
            customer_id TEXT,
            product_id TEXT,
            store_id TEXT,
            campaign_id TEXT,
            quantity INTEGER,
            sales_amount REAL,
            FOREIGN KEY (customer_id) REFERENCES customers (customer_id),
            FOREIGN KEY (product_id) REFERENCES products (product_id)
        );
    """)


def delete_existing_records(cursor: sqlite3.Cursor) -> None:
    """Drop existing tables to ensure clean slate."""
    cursor.execute("DROP TABLE IF EXISTS sales")
    cursor.execute("DROP TABLE IF EXISTS customers")
    cursor.execute("DROP TABLE IF EXISTS products")


def insert_customers(customers_df: pd.DataFrame, cursor: sqlite3.Cursor) -> None:
    """Insert customer data into the customers table."""
    # Rename columns to match database schema (lowercase with underscores)
    customers_df = customers_df.rename(
        columns={
            "CustomerID": "customer_id",
            "Name": "name",
            "Region": "region",
            "JoinDate": "join_date",
            "Age": "age",
            "SubscriptionStatus": "subscription_status",
        }
    )
    customers_df.to_sql("customers", cursor.connection, if_exists="append", index=False)


def insert_products(products_df: pd.DataFrame, cursor: sqlite3.Cursor) -> None:
    """Insert product data into the products table."""
    # Rename columns to match database schema
    products_df = products_df.rename(
        columns={
            "productid": "product_id",
            "productname": "product_name",
            "category": "category",
            "unitprice": "unit_price",
            "manufactureyear": "manufacture_year",
            "availabilitystatus": "availability_status",
        }
    )
    # Select only the columns we need
    products_df = products_df[
        [
            "product_id",
            "product_name",
            "category",
            "unit_price",
            "manufacture_year",
            "availability_status",
        ]
    ]
    products_df.to_sql("products", cursor.connection, if_exists="append", index=False)


def insert_sales(sales_df: pd.DataFrame, cursor: sqlite3.Cursor) -> None:
    """Insert sales data into the sales table."""
    # Rename columns to match database schema
    sales_df = sales_df.rename(
        columns={
            "TransactionID": "sale_id",
            "SaleDate": "sale_date",
            "CustomerID": "customer_id",
            "ProductID": "product_id",
            "StoreID": "store_id",
            "CampaignID": "campaign_id",
            "SaleAmount": "sales_amount",
            "DiscountAmount": "discount_amount",
            "State": "state",
        }
    )
    # Select only the columns we need
    sales_df = sales_df[
        [
            "sale_id",
            "sale_date",
            "customer_id",
            "product_id",
            "store_id",
            "campaign_id",
            "sales_amount",
            "discount_amount",
            "state",
        ]
    ]
    sales_df.to_sql("sales", cursor.connection, if_exists="append", index=False)


def load_data_to_db() -> None:
    """Main function to create schema and load data into the data warehouse."""
    try:
        # Ensure the data warehouse directory exists
        DW_DIR.mkdir(parents=True, exist_ok=True)

        # Connect to SQLite – will create the file if it doesn't exist
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Create schema and clear existing records
        print("Creating schema...")
        create_schema(cursor)
        print("Deleting existing records...")
        delete_existing_records(cursor)

        # Load prepared data using pandas
        print("Loading CSV files...")
        customers_df = pd.read_csv(PREPARED_DATA_DIR.joinpath("customers_prepared.csv"))
        products_df = pd.read_csv(PREPARED_DATA_DIR.joinpath("products_prepared.csv"))
        sales_df = pd.read_csv(PREPARED_DATA_DIR.joinpath("sales_prepared.csv"))

        # Insert data into the database
        print("Inserting customers...")
        insert_customers(customers_df, cursor)
        print("Inserting products...")
        insert_products(products_df, cursor)
        print("Inserting sales...")
        insert_sales(sales_df, cursor)

        conn.commit()
        print("Data warehouse loaded successfully!")

    except Exception as e:
        print(f"Error loading data: {e}")
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    load_data_to_db()
