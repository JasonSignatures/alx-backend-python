import sqlite3

def stream_users_in_batches(batch_size):
    """
    Generator that streams rows from 'user_data' in batches of given size.
    Yields a list of rows per batch.
    """
    conn = sqlite3.connect('database.db')
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM user_data")

    batch = []
    for row in cursor:  # 1st loop
        batch.append(row)
        if len(batch) == batch_size:
            yield batch
            batch = []
    # yield any remaining rows
    if batch:
        yield batch

    conn.close()


def batch_processing(batch_size):
    """
    Processes each batch to filter users over the age of 25.
    Yields filtered users one by one.
    """
    for batch in stream_users_in_batches(batch_size):  # 2nd loop
        for user in batch:  # 3rd loop
            # Assuming user schema: (id, name, age, ...)
            if user[2] > 25:
                yield user


# Optional test section
if __name__ == "__main__":
    for user in batch_processing(5):
        print(user)
