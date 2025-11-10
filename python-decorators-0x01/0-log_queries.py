def log_queries(func):
    def wrapper(*args, **kwargs):
        # logging or print statements here
        result = func(*args, **kwargs)
        return result
    return wrapper
