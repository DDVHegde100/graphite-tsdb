"""Graphite TSDB — embeddable time-series database for financial tick data."""

from graphite_tsdb import DB

__all__ = ["DB"]
__version__ = "0.1.0"

def open(path: str) -> DB:
    """Open or create a Graphite database at the given path."""
    return DB.open(path)
