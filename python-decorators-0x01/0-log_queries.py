@log_queries
def run_query(query, params=None):
    # Simulated database execution
    print("Running query in database...")
    return "Success"

# Example call
run_query("SELECT * FROM users WHERE id = 1;")
