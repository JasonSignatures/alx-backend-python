import sqlite3

def stream_user_ages():
    """
    Generator that streams user ages one by one from the database.
    Yields each age lazily to save memory.
    """
    conn = sqlite3.connect('database.db')
    cursor = conn.cursor()
    cursor.execute("SELECT age FROM user_data")

    for (age,) in cursor:  # ✅ 1st (and only) loop
        yield age  # Return one age at a time lazily

    conn.close()


def compute_average_age():
    """
    Computes the average age using the generator without loading
    all data into memory. Uses no more than two loops.
    """
    total_age = 0
    count = 0

    # ✅ 2nd loop: consumes the generator lazily
    for age in stream_user_ages():
        total_age += age
        count += 1

    average_age = total_age / count if count > 0 else 0
    print(f"Average age of users: {average_age:.2f}")


# Run the computation
if __name__ == "__main__":
    compute_average_age()
