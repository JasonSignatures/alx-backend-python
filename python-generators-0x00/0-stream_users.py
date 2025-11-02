import sqlite3

def stream_users():
    """
    Generator function that streams rows from the 'user_data' table one by one.
    Uses yield to return each row lazily.
    """
    # Connect to your database (replace 'database.db' with your actual DB)
    conn = sqlite3.connect('database.db')
    cursor = conn.cursor()

    # Execute a query to fetch all users
    cursor.execute("SELECT * FROM user_data")

    # Stream rows one by one
    for row in cursor:
        yield row  # Yield each row instead of returning all at once

    # Close connection after all rows are streamed
    conn.close()


# Optional: Example usage (for testing)
if __name__ == "__main__":
    for user in stream_users():
        print(user)
