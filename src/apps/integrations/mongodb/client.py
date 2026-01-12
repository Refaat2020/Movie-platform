"""
MongoDB client with connection pooling and error handling.

Provides thread-safe MongoDB connections for sync operations.
"""
import logging
from typing import Optional
from pymongo import MongoClient
from pymongo.database import Database
from pymongo.collection import Collection
from pymongo.errors import (
    ConnectionFailure,
    ServerSelectionTimeoutError,
    OperationFailure,
)
from django.conf import settings

logger = logging.getLogger(__name__)


class MongoDBClient:
    """
    Thread-safe MongoDB client wrapper.
    
    Features:
    - Connection pooling
    - Automatic retry on transient errors
    - Read preference configuration
    - Database/collection caching
    
    Usage:
        client = MongoDBClient()
        movies_collection = client.get_collection('movies')
        movies_collection.insert_one(doc)
    """
    
    _instance: Optional['MongoDBClient'] = None
    _client: Optional[MongoClient] = None
    
    def __new__(cls):
        """Singleton pattern for connection pooling."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        """Initialize MongoDB client if not already initialized."""
        if self._client is None:
            self._connect()
    
    def _connect(self):
        """
        Establish MongoDB connection with retry logic.
        
        Connection string format:
        mongodb://username:password@host:port/database?authSource=admin
        """
        mongodb_uri = settings.MONGODB_URI
        
        if not mongodb_uri:
            raise ValueError("MONGODB_URI not configured in settings")
        
        try:
            logger.info("Connecting to MongoDB...")
            
            self._client = MongoClient(
                mongodb_uri,
                # Connection pooling
                maxPoolSize=50,
                minPoolSize=10,
                # Timeout settings
                serverSelectionTimeoutMS=5000,
                connectTimeoutMS=10000,
                socketTimeoutMS=30000,
                # Retry settings
                retryWrites=True,
                retryReads=True,
                # Application name for monitoring
                appName='movie_platform',
            )
            
            # Test connection
            self._client.admin.command('ping')
            
            logger.info("✓ MongoDB connection established")
        
        except (ConnectionFailure, ServerSelectionTimeoutError) as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            raise
    
    def get_database(self) -> Database:
        """
        Get MongoDB database instance.
        
        Returns:
            MongoDB database object
        """
        if self._client is None:
            self._connect()
        
        # Extract database name from URI or use default
        db_name = getattr(settings, 'MONGODB_DATABASE', 'movies_reporting')
        return self._client[db_name]
    
    def get_collection(self, collection_name: str) -> Collection:
        """
        Get MongoDB collection instance.
        
        Args:
            collection_name: Name of the collection
        
        Returns:
            MongoDB collection object
        """
        db = self.get_database()
        return db[collection_name]
    
    def ensure_indexes(self):
        """
        Ensure all required indexes exist on collections.
        
        Should be called during application startup or deployment.
        Creates indexes if they don't exist (idempotent).
        """
        logger.info("Ensuring MongoDB indexes...")
        
        try:
            # Movies collection indexes
            movies = self.get_collection('movies')
            
            # Primary lookup index
            movies.create_index(
                'movie_id',
                unique=True,
                name='idx_movie_id'
            )
            
            # TMDB ID index (for external movie lookups)
            movies.create_index(
                'tmdb_id',
                sparse=True,  # Only index documents with tmdb_id
                name='idx_tmdb_id'
            )
            
            # Text search index (title and overview)
            movies.create_index(
                [('title', 'text'), ('overview', 'text')],
                name='idx_text_search',
                default_language='english'
            )
            
            # Sorting and filtering indexes
            movies.create_index(
                [('popularity', -1), ('release_date', -1)],
                name='idx_popularity_date'
            )
            
            movies.create_index(
                [('vote_average', -1), ('vote_count', -1)],
                name='idx_rating_votes'
            )
            
            movies.create_index(
                'release_date',
                name='idx_release_date'
            )
            
            # Genre filtering (multi-key index for array field)
            movies.create_index(
                'genres.slug',
                name='idx_genre_slug'
            )
            
            # Status and source filters
            movies.create_index(
                [('status', 1), ('created_at', -1)],
                name='idx_status_created'
            )
            
            movies.create_index(
                [('source', 1), ('popularity', -1)],
                name='idx_source_popularity'
            )
            
            # Timestamp indexes for sync monitoring
            movies.create_index(
                'synced_at',
                name='idx_synced_at'
            )
            
            movies.create_index(
                'created_at',
                name='idx_created_at'
            )
            
            logger.info("✓ Movies collection indexes created")
            
            # Reviews collection indexes
            reviews = self.get_collection('reviews')
            
            # Primary lookup
            reviews.create_index(
                'review_id',
                unique=True,
                name='idx_review_id'
            )
            
            # Movie reviews lookup
            reviews.create_index(
                [('movie_id', 1), ('created_at', -1)],
                name='idx_movie_created'
            )
            
            # Top reviews by helpfulness
            reviews.create_index(
                [('movie_id', 1), ('helpful_count', -1)],
                name='idx_movie_helpful'
            )
            
            # Rating distribution queries
            reviews.create_index(
                [('movie_id', 1), ('rating', 1)],
                name='idx_movie_rating'
            )
            
            # User reviews lookup
            reviews.create_index(
                [('user_id', 1), ('created_at', -1)],
                name='idx_user_created'
            )
            
            logger.info("✓ Reviews collection indexes created")
            
            # Analytics cache collection
            analytics = self.get_collection('analytics_cache')
            
            # Cache key lookup
            analytics.create_index(
                'cache_key',
                unique=True,
                name='idx_cache_key'
            )
            
            # TTL index (auto-delete expired cache)
            analytics.create_index(
                'expires_at',
                expireAfterSeconds=0,  # Delete when expires_at is reached
                name='idx_expires_at'
            )
            
            # Cache type queries
            analytics.create_index(
                [('cache_type', 1), ('computed_at', -1)],
                name='idx_cache_type'
            )
            
            logger.info("✓ Analytics cache collection indexes created")
            logger.info("✓ All MongoDB indexes ensured")
        
        except OperationFailure as e:
            logger.error(f"Failed to create indexes: {e}")
            raise
    
    def health_check(self) -> bool:
        """
        Check MongoDB connection health.
        
        Returns:
            True if connection is healthy
        """
        try:
            if self._client is None:
                return False
            
            self._client.admin.command('ping')
            return True
        
        except Exception as e:
            logger.error(f"MongoDB health check failed: {e}")
            return False
    
    def close(self):
        """Close MongoDB connection."""
        if self._client:
            logger.info("Closing MongoDB connection...")
            self._client.close()
            self._client = None
    
    def __enter__(self):
        """Context manager support."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager cleanup."""
        # Don't close on exit (keep connection pool alive)
        pass


# Singleton instance
mongodb_client = MongoDBClient()
