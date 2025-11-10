import functools

def log_queries(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        # Extract query argument if it exists
        query = kwargs.get("query") or (args[0] if args else None)
        
        # Log the SQL query before execution
        if query:
            print(f"[LOG] Executing SQL Query: {query}")
        else:
            print("[LOG] Executing function without explicit SQL query argument.")
        
        # Execute the actual function
        result = func(*args, **kwargs)
        
        print("[LOG] Query execution completed.")
        return result
    return wrapper
