"""
FastAPI dependency injection.

Provides:
- MongoDB client (singleton)
- Database access
- Collection access
"""
from pymongo import MongoClient
from pymongo.database import Database
from pymongo.collection import Collection
from functools import lru_cache

from reporting_api.config import settings


# Singleton MongoDB client
_mongodb_client: MongoClient = None


def get_mongodb_client() -> MongoClient:
    """
    Get MongoDB client (singleton).
    
    Returns:
        MongoClient instance
    """
    global _mongodb_client
    
    if _mongodb_client is None:
        _mongodb_client = MongoClient(
            settings.MONGODB_URI,
            maxPoolSize=50,
            minPoolSize=10,
            serverSelectionTimeoutMS=5000,
        )
    
    return _mongodb_client


def get_database() -> Database:
    """
    Get MongoDB database.
    
    Returns:
        Database instance
    """
    client = get_mongodb_client()
    return client[settings.MONGODB_DATABASE]


def get_movies_collection() -> Collection:
    """
    Get movies collection.
    
    Returns:
        Collection instance
    
    Usage:
        @app.get("/movies")
        async def get_movies():
            collection = get_movies_collection()
            return list(collection.find().limit(10))
    """
    db = get_database()
    return db['movies']


def get_reviews_collection() -> Collection:
    """
    Get reviews collection.
    
    Returns:
        Collection instance
    """
    db = get_database()
    return db['reviews']
