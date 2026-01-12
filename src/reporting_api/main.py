"""
FastAPI Reporting Service - Read-only MongoDB Analytics.

Features:
- High-performance read queries
- MongoDB aggregation pipelines
- Typed responses with Pydantic
- API documentation (Swagger/ReDoc)
- Error handling
- Health checks
"""
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
import logging

from reporting_api.config import settings
from reporting_api.routers import movies, health
from reporting_api.dependencies import get_mongodb_client

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application lifespan manager.
    
    Handles:
    - MongoDB connection on startup
    - Index verification
    - Cleanup on shutdown
    """
    # Startup
    logger.info("Starting FastAPI Reporting Service...")
    
    try:
        # Verify MongoDB connection
        client = get_mongodb_client()
        client.admin.command('ping')
        logger.info("✓ MongoDB connection established")
        
        # Verify indexes exist (optional - can create if missing)
        # Indexes should be created by Django management command
        db = client[settings.MONGODB_DATABASE]
        movies_collection = db['movies']
        
        indexes = list(movies_collection.list_indexes())
        logger.info(f"✓ Found {len(indexes)} indexes on movies collection")
        
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {e}")
        # Decide: fail fast or continue with degraded service
        # raise  # Uncomment to fail fast
    
    yield
    
    # Shutdown
    logger.info("Shutting down FastAPI Reporting Service...")
    try:
        client = get_mongodb_client()
        client.close()
        logger.info("✓ MongoDB connection closed")
    except Exception as e:
        logger.error(f"Error closing MongoDB connection: {e}")


# Create FastAPI app
app = FastAPI(
    title="Movie Platform Reporting API",
    description="Read-only analytics and reporting service powered by MongoDB",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "HEAD", "OPTIONS"],  # Read-only
    allow_headers=["*"],
)

# Include routers
app.include_router(health.router, prefix="/health", tags=["Health"])
app.include_router(movies.router, prefix="/report", tags=["Reports"])


# Global exception handler
@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    """Handle unexpected exceptions gracefully."""
    logger.error(f"Unexpected error: {exc}", exc_info=True)
    
    return JSONResponse(
        status_code=500,
        content={
            "error": {
                "code": "INTERNAL_SERVER_ERROR",
                "message": "An unexpected error occurred",
                "detail": str(exc) if settings.DEBUG else None,
            }
        }
    )


# Root endpoint
@app.get("/", tags=["Root"])
async def root():
    """API root with service information."""
    return {
        "service": "Movie Platform Reporting API",
        "version": "1.0.0",
        "description": "Read-only analytics powered by MongoDB",
        "endpoints": {
            "docs": "/docs",
            "health": "/health/",
            "reports": "/report/",
        }
    }
