"""
Health check endpoints for monitoring.
"""
from fastapi import APIRouter
import logging

from reporting_api.dependencies import get_mongodb_client, get_movies_collection

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/", summary="Health check")
async def health_check():
    """
    Basic health check.
    
    Returns:
        Service status
    """
    return {
        "status": "healthy",
        "service": "Movie Reporting API"
    }


@router.get("/mongodb", summary="MongoDB connection check")
async def mongodb_health():
    """
    Check MongoDB connection health.
    
    Returns:
        MongoDB connection status
    """
    try:
        client = get_mongodb_client()
        # Ping database
        client.admin.command('ping')
        
        # Count documents in movies collection
        collection = get_movies_collection()
        count = collection.count_documents({})
        
        return {
            "status": "healthy",
            "database": "connected",
            "movies_count": count
        }
    
    except Exception as e:
        logger.error(f"MongoDB health check failed: {e}")
        return {
            "status": "unhealthy",
            "database": "disconnected",
            "error": str(e)
        }
