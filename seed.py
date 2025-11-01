# The 'mysql.connector' library is required to connect and interact with MySQL.
# You must install it using: pip install mysql-connector-python
import mysql.connector
import csv
import uuid
from typing import Generator, Tuple, Dict, Any

# --- Database Connection Configuration (Update these values) ---
# NOTE: Replace 'your_user', 'your_password', and 'your_host' 
# with your actual MySQL credentials.
DB_CONFIG = {
    'user': 'your_user',
    'password': 'your_password', 
    'host': 'localhost',
    'raise_on_warnings': True
}

DATABASE_NAME = 'ALX_prodev'
TABLE_NAME = 'user_data'

def connect_db(config: Dict[str, Any] = DB_CONFIG):
    """
    Connects to the MySQL database server.
    Returns the connection object or None if connection fails.
    """
    try:
        connection = mysql.connector.connect(**config)
        print("Successfully connected to MySQL server.")
        return connection
    except mysql.connector.Error as err:
        print(f"Error connecting to MySQL: {err}")
        return None

def create_database(connection: mysql.connector.connection.MySQLConnection):
    """
    Creates the database ALX_prodev if it does not exist.
    """
    cursor = connection.cursor()
    try:
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME} DEFAULT CHARACTER SET 'utf8'")
        connection.database = DATABASE_NAME
        print(f"Database {DATABASE_NAME} ensured to exist.")
    except mysql.connector.Error as err:
        print(f"Failed creating database: {err}")
    finally:
        cursor.close()

def connect_to_prodev(config: Dict[str, Any] = DB_CONFIG):
    """
    Connects to the ALX_prodev database in MySQL.
    """
    try:
        config_with_db = config.copy()
        config_with_db['database'] = DATABASE_NAME
        connection = mysql.connector.connect(**config_with_db)
        print(f"Successfully connected to database {DATABASE_NAME}.")
        return connection
    except mysql.connector.Error as err:
        # If the database doesn't exist yet, connect to the server only
        if err.errno == mysql.connector.errorcode.ER_BAD_DB_ERROR:
            return connect_db(config)
        print(f"Error connecting to {DATABASE_NAME}: {err}")
        return None

def create_table(connection: mysql.connector.connection.MySQLConnection):
    """
    Creates the user_data table if it does not exist with the required fields.
    """
    TABLE_SCHEMA = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        user_id CHAR(36) NOT NULL,
        name VARCHAR(255) NOT NULL,
        email VARCHAR(255) NOT NULL,
        age DECIMAL(3, 0) NOT NULL,
        PRIMARY KEY (user_id),
        INDEX idx_user_id (user_id)
    );
    """
    cursor = connection.cursor()
    try:
        cursor.execute(TABLE_SCHEMA)
        connection.commit()
        print(f"Table {TABLE_NAME} created successfully")
    except mysql.connector.Error as err:
        print(f"Failed creating table: {err}")
    finally:
        cursor.close()

def insert_data(connection: mysql.connector.connection.MySQLConnection, csv_filepath: str):
    """
    Inserts data from a CSV file into the user_data table.
    It checks if the table is empty before insertion.
    """
    cursor = connection.cursor(buffered=True)
    
    # 1. Check if table is empty
    cursor.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}")
    if cursor.fetchone()[0] > 0:
        print(f"Table {TABLE_NAME} already contains data. Skipping insertion.")
        cursor.close()
        return

    # 2. Insert data from CSV
    print(f"Inserting data from {csv_filepath}...")
    INSERT_QUERY = f"INSERT INTO {TABLE_NAME} (user_id, name, email, age) VALUES (%s, %s, %s, %s)"
    
    try:
        with open(csv_filepath, 'r', newline='') as file:
            reader = csv.reader(file)
            next(reader)  # Skip the header row
            
            records_to_insert = []
            for row in reader:
                # Generate a UUID for the Primary Key
                user_id = str(uuid.uuid4())
                name, email, age = row[0], row[1], row[2] 
                
                # Check for empty age value before casting
                if not age:
                    age_val = 0 # Default or error handling for missing age
                else:
                    try:
                        age_val = int(age)
                    except ValueError:
                        age_val = 0 # Default or error handling for non-numeric age
                
                records_to_insert.append((user_id, name, email, age_val))

            # Execute batch insert
            cursor.executemany(INSERT_QUERY, records_to_insert)
            connection.commit()
            print(f"Successfully inserted {cursor.rowcount} records.")

    except FileNotFoundError:
        print(f"Error: The file {csv_filepath} was not found.")
    except Exception as e:
        print(f"An error occurred during data insertion: {e}")
        connection.rollback()
    finally:
        cursor.close()

# --- Objective: Generator Function ---

def stream_data_generator(connection: mysql.connector.connection.MySQLConnection) -> Generator[Tuple, Any, None]:
    """
    A generator that streams rows from the user_data table one by one.

    This function uses the 'SSCursor' (or Cursors with buffered=False) feature 
    to retrieve results without loading the entire dataset into memory at once,
    achieving true streaming for large datasets.
    """
    # Use a server-side cursor (SSCursor) for streaming large results
    # Buffered=False is crucial here for memory efficiency
    cursor = connection.cursor(buffered=False)
    
    try:
        print(f"\n--- Initiating Generator Stream from {TABLE_NAME} ---")
        QUERY = f"SELECT user_id, name, email, age FROM {TABLE_NAME} ORDER BY age ASC"
        cursor.execute(QUERY)
        
        # The generator yields rows as they are fetched from the server
        for row in cursor:
            yield row
            
    except mysql.connector.Error as err:
        print(f"Error during data streaming: {err}")
        
    finally:
        # Crucial: The cursor must be closed after the generator finishes
        cursor.close()
        print("--- Generator Stream Complete and Cursor Closed ---")


if __name__ == '__main__':
    # Placeholder for the user_data.csv file creation (necessary for the script to run)
    # This block ensures the CSV exists before the script tries to read it.
    csv_filename = 'user_data.csv'
    try:
        with open(csv_filename, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['name', 'email', 'age'])
            # Sample data (you should replace this with your full dataset)
            writer.writerow(['Alice Smith', 'alice@example.com', 35])
            writer.writerow(['Bob Johnson', 'bob@example.com', 22])
            writer.writerow(['Charlie Brown', 'charlie@example.com', 48])
            writer.writerow(['Dan Altenwerth Jr.', 'Molly59@gmail.com', 67])
            writer.writerow(['Glenda Wisozk', 'Miriam21@gmail.com', 119])
            writer.writerow(['Daniel Fahey IV', 'Delia.Lesch11@hotmail.com', 49])
            writer.writerow(['Ronnie Bechtelar', 'Sandra19@yahoo.com', 22])
            writer.writerow(['Alma Bechtelar', 'Shelly_Balistreri22@hotmail.com', 102])
        print(f"Created sample '{csv_filename}' file for insertion.")
    except Exception as e:
        print(f"Could not create sample CSV file: {e}")

    # Example of how to use the generator (demonstration)
    conn = connect_db()
    if conn:
        create_database(conn)
        conn.close() # Close server connection

        conn_prodev = connect_to_prodev()
        if conn_prodev:
            create_table(conn_prodev)
            insert_data(conn_prodev, csv_filename)

            # Test the generator function
            try:
                data_stream = stream_data_generator(conn_prodev)
                
                print("First 3 records streamed:")
                for i in range(3):
                    try:
                        record = next(data_stream)
                        print(f"  -> {record}")
                    except StopIteration:
                        print("Stream finished early.")
                        break
                        
                # You can continue iterating later...
                print("\nRemaining records streamed (lazy evaluation):")
                for record in data_stream:
                    print(f"  -> {record}")
                    
            except Exception as e:
                print(f"Generator usage error: {e}")
                
            finally:
                conn_prodev.close()
                print("\nDatabase connection closed.")
        else:
            print("Could not connect to ALX_prodev database.")
