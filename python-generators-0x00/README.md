Python Generators for Database Streaming

This project demonstrates the use of Python generator functions to efficiently stream large datasets from a MySQL database without loading the entire result set into application memory. This approach is critical for handling Big Data scenarios where memory consumption must be minimized.

Files

seed.py: Contains all the necessary functions for database connection, schema creation, data insertion from a CSV, and the core generator function (stream_data_generator).

0-main.py (External Driver): The driver script used to execute the setup and demonstrate the initial data fetch (which is not streaming) and the generator usage.

Database Schema

The script is designed for the ALX_prodev database and the user_data table.

Field

Type

Constraint

Purpose

user_id

CHAR(36)

PRIMARY KEY, INDEX

Unique identifier (UUID).

name

VARCHAR(255)

NOT NULL

User's full name.

email

VARCHAR(255)

NOT NULL

User's email address.

age

DECIMAL(3, 0)

NOT NULL

User's age.

Key Objective: Memory Efficiency (stream_data_generator)

The function stream_data_generator(connection) is the core of this project, demonstrating true data streaming:

Server-Side Cursor: The function initializes the MySQL cursor with buffered=False. This is a crucial step that tells the mysql.connector to use a server-side cursor (or unbuffered mode).

Lazy Evaluation: This configuration prevents the MySQL connector from buffering all rows. Instead, the Python generator yields rows one by one, directly fetching them from the database server only when the loop requests next().

Result: This ensures a minimal, constant memory footprint, making it ideal for processing tables with millions or billions of rows.

Prerequisites

MySQL Server: A running MySQL instance.

Python Library: The official MySQL connector for Python.

pip install mysql-connector-python


Configuration: You must update the DB_CONFIG dictionary in seed.py with your personal MySQL credentials (user, password, host) before running.
