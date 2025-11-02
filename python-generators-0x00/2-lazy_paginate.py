import sqlite3

def paginate_users(page_size, offset):
    """
    Fetch a single page of users from the database starting at the given offset.
    Returns a list of rows limited by the page_size.
    """
    conn = sqlite3.connect('database.db')
    cursor = conn.cursor()
    cursor.execute(
        "SELECT * FROM user_data LIMIT ? OFFSET ?",
        (page_size, offset)
    )
    rows = cursor.fetchall()
    conn.close()
    return rows


def lazy_paginate(page_size):
    """
    Generator that lazily fetches paginated data from the 'user_data' table.
    Fetches one page at a time, only when needed.
    Uses only one loop.
    """
    offset = 0
    while True:  # ✅ Only one loop
        page = paginate_users(page_size, offset)
        if not page:  # Stop when there are no more rows
            break
