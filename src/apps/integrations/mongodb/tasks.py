"""
Celery tasks for syncing PostgreSQL data to MongoDB.

Design Principles:
- Idempotent: Safe to retry
- Async: Don't block API responses
- Batched: Efficient bulk operations
- Monitored: Track sync lag and failures
"""
import logging
from typing import List, Dict, Any
from celery import shared_task, group
from django.db import transaction

from apps.integrations.mongodb.sync import SyncRecovery


from src.apps.movies.models import Movie, Review
from src.apps.integrations.mongodb.repository import (
    MongoMovieRepository,
    MongoReviewRepository,
)

logger = logging.getLogger(__name__)


@shared_task(
    bind=True,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_backoff_max=300,  # Max 5 minutes between retries
    max_retries=3,
    retry_jitter=True,
)
def sync_movie_to_mongodb(self, movie_id: str) -> Dict[str, Any]:
    """
    Sync a single movie from PostgreSQL to MongoDB.
    
    Triggered by:
    - Django signals (post_save)
    - Manual triggers
    - Batch sync operations
    
    Args:
        movie_id: Django Movie UUID
    
    Returns:
        Dictionary with sync result
    
    Example:
        # Async
        sync_movie_to_mongodb.delay(movie_id='abc-123')
        
        # Sync (for testing)
        result = sync_movie_to_mongodb(movie_id='abc-123')
    """
    logger.info(f"Syncing movie {movie_id} to MongoDB")
    
    try:
        # Fetch movie from PostgreSQL with related data
        # Use select_related/prefetch_related for efficiency
        movie = Movie.objects.select_related().prefetch_related(
            'genres',
            # 'production_companies',
            'reviews'
        ).get(id=movie_id)
        
        # Transform to MongoDB document
        movie_doc = MongoMovieRepository.transform_movie_to_document(movie)
        
        # Upsert to MongoDB
        repo = MongoMovieRepository()
        repo.upsert_movie(movie_doc)
        
        # Update sync timestamp in PostgreSQL
        movie.mark_synced()
        
        logger.info(f"✓ Synced movie {movie_id} ({movie.title}) to MongoDB")
        
        return {
            'movie_id': movie_id,
            'title': movie.title,
            'synced': True,
        }
    
    except Movie.DoesNotExist:
        logger.warning(f"Movie {movie_id} not found in PostgreSQL")
        return {
            'movie_id': movie_id,
            'synced': False,
            'error': 'not_found',
        }
    
    except Exception as e:
        logger.error(
            f"Failed to sync movie {movie_id} to MongoDB",
            exc_info=True
        )
        raise


@shared_task(
    bind=True,
    autoretry_for=(Exception,),
    retry_backoff=True,
    max_retries=3,
)
def delete_movie_from_mongodb(self, movie_id: str) -> Dict[str, Any]:
    """
    Delete a movie from MongoDB.
    
    Triggered by:
    - Django signals (post_delete)
    - Soft delete operations (when is_active=False)
    
    Args:
        movie_id: Django Movie UUID
    
    Returns:
        Dictionary with deletion result
    """
    logger.info(f"Deleting movie {movie_id} from MongoDB")
    
    try:
        repo = MongoMovieRepository()
        deleted = repo.delete_movie(movie_id)
        
        if deleted:
            logger.info(f"✓ Deleted movie {movie_id} from MongoDB")
        else:
            logger.warning(f"Movie {movie_id} not found in MongoDB")
        
        return {
            'movie_id': movie_id,
            'deleted': deleted,
        }
    
    except Exception as e:
        logger.error(
            f"Failed to delete movie {movie_id} from MongoDB",
            exc_info=True
        )
        raise


@shared_task(
    bind=True,
    autoretry_for=(Exception,),
    retry_backoff=True,
    max_retries=3,
)
def sync_review_to_mongodb(self, review_id: str) -> Dict[str, Any]:
    """
    Sync a single review from PostgreSQL to MongoDB.
    
    When a review is created/updated, we need to:
    1. Sync the review document
    2. Re-sync the parent movie (to update review_stats)
    
    Args:
        review_id: Django Review UUID
    
    Returns:
        Dictionary with sync result
    """
    logger.info(f"Syncing review {review_id} to MongoDB")
    
    try:
        # Fetch review with related movie
        review = Review.objects.select_related('movie', 'user').get(id=review_id)
        
        # Sync review document
        review_doc = MongoReviewRepository.transform_review_to_document(review)
        review_repo = MongoReviewRepository()
        review_repo.upsert_review(review_doc)
        
        # Also sync parent movie to update review_stats
        # This is important because review_stats are denormalized
        sync_movie_to_mongodb.delay(str(review.movie.id))
        
        logger.info(f"✓ Synced review {review_id} to MongoDB")
        
        return {
            'review_id': review_id,
            'movie_id': str(review.movie.id),
            'synced': True,
        }
    
    except Review.DoesNotExist:
        logger.warning(f"Review {review_id} not found in PostgreSQL")
        return {
            'review_id': review_id,
            'synced': False,
            'error': 'not_found',
        }
    
    except Exception as e:
        logger.error(
            f"Failed to sync review {review_id} to MongoDB",
            exc_info=True
        )
        raise


@shared_task(
    bind=True,
    autoretry_for=(Exception,),
    retry_backoff=True,
    max_retries=3,
)
def delete_review_from_mongodb(self, review_id: str, movie_id: str = None) -> Dict[str, Any]:
    """
    Delete a review from MongoDB.
    
    Also triggers re-sync of parent movie to update review_stats.
    
    Args:
        review_id: Django Review UUID
        movie_id: Django Movie UUID (optional, for re-syncing parent)
    
    Returns:
        Dictionary with deletion result
    """
    logger.info(f"Deleting review {review_id} from MongoDB")
    
    try:
        repo = MongoReviewRepository()
        deleted = repo.delete_review(review_id)
        
        # Re-sync parent movie if movie_id provided
        if movie_id:
            sync_movie_to_mongodb.delay(movie_id)
        
        logger.info(f"✓ Deleted review {review_id} from MongoDB")
        
        return {
            'review_id': review_id,
            'deleted': deleted,
        }
    
    except Exception as e:
        logger.error(
            f"Failed to delete review {review_id} from MongoDB",
            exc_info=True
        )
        raise


@shared_task(
    bind=True,
    autoretry_for=(Exception,),
    retry_backoff=True,
    max_retries=3,
)
def batch_sync_movies_to_mongodb(self, movie_ids: List[str]) -> Dict[str, Any]:
    """
    Batch sync multiple movies to MongoDB.
    
    Uses parallel task execution for speed.
    
    Args:
        movie_ids: List of Django Movie UUIDs
    
    Returns:
        Dictionary with batch sync statistics
    
    Example:
        batch_sync_movies_to_mongodb.delay(['uuid1', 'uuid2', 'uuid3'])
    """
    logger.info(f"Batch syncing {len(movie_ids)} movies to MongoDB")
    
    # Create group of parallel tasks
    job = group(
        sync_movie_to_mongodb.s(movie_id)
        for movie_id in movie_ids
    )
    
    # Execute tasks in parallel
    result = job.apply_async()
    
    # Wait for all tasks to complete (with timeout)
    results = result.get(timeout=300)  # 5 minute timeout
    
    # Aggregate statistics
    stats = {
        'total': len(movie_ids),
        'synced': sum(1 for r in results if r.get('synced')),
        'failed': sum(1 for r in results if not r.get('synced')),
    }
    
    logger.info(
        f"✓ Batch sync complete: {stats['synced']}/{stats['total']} movies synced"
    )
    
    return stats


@shared_task(
    bind=True,
    autoretry_for=(Exception,),
    retry_backoff=True,
    max_retries=3,
)
def sync_stale_movies_to_mongodb(self, limit: int = 100) -> Dict[str, Any]:
    """
    Find and sync movies that haven't been synced recently.
    
    This is a periodic task to ensure sync consistency.
    Should be scheduled to run every few minutes.
    
    Args:
        limit: Maximum number of movies to sync
    
    Returns:
        Dictionary with sync statistics
    """
    logger.info(f"Syncing up to {limit} stale movies to MongoDB")
    
    from django.db.models import Q, F
    
    # Find movies that need syncing:
    # 1. Never synced (synced_to_mongo_at is null)
    # 2. Synced before last update (synced_to_mongo_at < updated_at)
    stale_movies = Movie.objects.filter(
        Q(synced_to_mongo_at__isnull=True) |
        Q(synced_to_mongo_at__lt=F('updated_at')),
        is_active=True
    ).values_list('id', flat=True)[:limit]
    
    movie_ids = [str(movie_id) for movie_id in stale_movies]
    
    if not movie_ids:
        logger.info("No stale movies found")
        return {'synced': 0, 'total': 0}
    
    # Batch sync
    stats = batch_sync_movies_to_mongodb(movie_ids)
    
    logger.info(f"✓ Synced {stats['synced']} stale movies to MongoDB")
    
    return stats


@shared_task(
    bind=True,
    autoretry_for=(Exception,),
    retry_backoff=True,
    max_retries=3,
)
def full_resync_to_mongodb(self, batch_size: int = 100) -> Dict[str, Any]:
    """
    Full resync of all active movies to MongoDB.
    
    Use this for:
    - Initial MongoDB setup
    - Recovery after MongoDB data loss
    - Schema migrations
    
    WARNING: This can take a long time for large databases.
    
    Args:
        batch_size: Number of movies per batch
    
    Returns:
        Dictionary with sync statistics
    """
    logger.info("Starting full resync to MongoDB")
    
    # Get all active movies
    all_movie_ids = Movie.objects.filter(
        is_active=True
    ).values_list('id', flat=True)
    
    total_movies = len(all_movie_ids)
    logger.info(f"Found {total_movies} movies to sync")
    
    # Process in batches
    synced = 0
    failed = 0
    
    for i in range(0, total_movies, batch_size):
        batch = [str(id) for id in all_movie_ids[i:i + batch_size]]
        
        try:
            result = batch_sync_movies_to_mongodb(batch)
            synced += result['synced']
            failed += result['failed']
            
            logger.info(
                f"Progress: {synced + failed}/{total_movies} movies processed"
            )
        
        except Exception as e:
            logger.error(f"Batch sync failed: {e}")
            failed += len(batch)
    
    stats = {
        'total': total_movies,
        'synced': synced,
        'failed': failed,
    }
    
    logger.info(f"✓ Full resync complete: {stats}")
    
    return stats


@shared_task(
    bind=True,
    autoretry_for=(Exception,),
    retry_backoff=True,
    max_retries=3,
)
def monitor_sync_health(self) -> Dict[str, Any]:
    """
    Monitor MongoDB sync health.
    
    Checks:
    - Sync lag (movies not synced in last N minutes)
    - Total synced vs total active movies
    
    This task should be scheduled to run periodically
    and alert if sync lag is too high.
    
    Returns:
        Dictionary with health metrics
    """
    logger.info("Monitoring MongoDB sync health")
    
    from django.db.models import Q
    
    # Count active movies in PostgreSQL
    total_postgres = Movie.objects.filter(is_active=True).count()
    
    # Count never synced
    never_synced = Movie.objects.filter(
        is_active=True,
        synced_to_mongo_at__isnull=True
    ).count()
    
    # Count stale (synced before last update)
    stale = Movie.objects.filter(
        is_active=True,
        synced_to_mongo_at__isnull=False,
        synced_to_mongo_at__lt=Q(updated_at=F('updated_at'))
    ).count()
    
    # MongoDB sync lag
    repo = MongoMovieRepository()
    mongo_lag = repo.get_sync_lag_count(minutes=5)
    
    health = {
        'total_postgres_movies': total_postgres,
        'never_synced': never_synced,
        'stale_movies': stale,
        'mongo_sync_lag_5min': mongo_lag,
        'sync_health': 'good' if (never_synced + stale) < 10 else 'degraded',
    }
    
    if health['sync_health'] == 'degraded':
        logger.warning(
            f"Sync health degraded: {never_synced} never synced, "
            f"{stale} stale, {mongo_lag} MongoDB lag"
        )
    else:
        logger.info(f"Sync health good: {health}")
    
    return health


@shared_task(
    bind=True,
    autoretry_for=(Exception,),
    retry_kwargs={"max_retries": 3, "countdown": 30},
    queue="mongodb_sync"
)
def sync_movies_to_mongodb(self, limit: int | None = None):
    """
    Sync movies from PostgreSQL to MongoDB
    """
    logger.info("🚀 Starting MongoDB sync")

    stats = bulk_sync_movies(limit=limit)

    logger.info(
        "✅ MongoDB sync completed",
        extra=stats
    )

    return stats