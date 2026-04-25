import os
import psycopg2
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())


def get_oltp_connection():
    return psycopg2.connect(
        host=os.getenv("OLTP_DB_HOST", "localhost"),
        port=int(os.getenv("OLTP_DB_PORT", 5432)),
        dbname=os.getenv("OLTP_DB_NAME"),
        user=os.getenv("OLTP_DB_USER"),
        password=os.getenv("OLTP_DB_PASSWORD")
    )


def get_analytical_connection():
    return psycopg2.connect(
        host=os.getenv("ANALYTICAL_DB_HOST", "localhost"),
        port=int(os.getenv("ANALYTICAL_DB_PORT", 5432)),
        dbname=os.getenv("ANALYTICAL_DB_NAME"),
        user=os.getenv("ANALYTICAL_DB_USER"),
        password=os.getenv("ANALYTICAL_DB_PASSWORD")
    )
