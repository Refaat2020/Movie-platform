"""
MongoDB integration package.

Provides sync layer for replicating PostgreSQL data to MongoDB
for high-performance analytics queries.
"""
from src.apps.integrations.mongodb.client import mongodb_client

__all__ = ['mongodb_client']
