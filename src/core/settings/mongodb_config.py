"""
MongoDB configuration for Django settings.

Add to your base settings:
    from .mongodb_config import *
"""
import os

# MongoDB connection URI
MONGODB_URI = os.getenv(
    'MONGODB_URI',
    'mongodb://admin:admin@localhost:27017/movies_reporting?authSource=admin'
)

# MongoDB database name
MONGODB_DATABASE = os.getenv('MONGODB_DATABASE', 'movies_reporting')

# Enable/disable MongoDB sync
# Set to False to disable sync (useful for testing, migrations)
ENABLE_MONGODB_SYNC = os.getenv('ENABLE_MONGODB_SYNC', 'True').lower() == 'true'

# MongoDB connection pool settings (applied in client.py)
MONGODB_MAX_POOL_SIZE = int(os.getenv('MONGODB_MAX_POOL_SIZE', '50'))
MONGODB_MIN_POOL_SIZE = int(os.getenv('MONGODB_MIN_POOL_SIZE', '10'))

# Sync batch size for bulk operations
MONGODB_SYNC_BATCH_SIZE = int(os.getenv('MONGODB_SYNC_BATCH_SIZE', '100'))
