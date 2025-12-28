# recon/db.py
import duckdb
from contextlib import contextmanager

@contextmanager
def duckdb_session():
    """
    Creates an in-memory DuckDB session.
    Everything disappears when the context exits.
    """
    con = duckdb.connect()
    try:
        yield con
    finally:
        con.close()
