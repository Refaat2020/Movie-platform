"""
Celery tasks for TMDB data ingestion.

Tasks:
- ingest_popular_movies: Periodic task to fetch popular movies
- ingest_movie_detail: Fetch detailed info for specific movie
- refresh_genres: Sync TMDB genres to database
- batch_ingest_movies: Ingest multiple movies by ID

Features:
- Idempotent operations (safe to retry)
- Exponential backoff on failures
- Rate limit handling
- Transactional safety
- Comprehensive logging
- Ingestion tracking
"""
import logging
from typing import List, Dict, Any, Optional
from decimal import Decimal
from datetime import datetime

from celery import shared_task, group
from django.db import transaction, IntegrityError
from django.utils import timezone
from django.conf import settings

from src.apps.integrations.tmdb.client import TMDBClient
from src.apps.integrations.tmdb.exceptions import (
    TMDBAPIError,
    TMDBRateLimitError,
    TMDBAuthError,
    TMDBNotFoundError,
)
from src.apps.movies.models import Movie, Genre, IngestionLog
from src.apps.movies.repositories.movie_repository import MovieRepository

logger = logging.getLogger(__name__)


@shared_task(
    bind=True,
    autoretry_for=(TMDBAPIError, TMDBRateLimitError),
    retry_backoff=True,
    retry_backoff_max=600,  # Max 10 minutes
    max_retries=5,
    retry_jitter=True,
    acks_late=True,  # Acknowledge after task completes
    reject_on_worker_lost=True,
)
def ingest_popular_movies(
    self,
    pages: int = 5,
    region: Optional[str] = None
) -> Dict[str, Any]:
    """
    Fetch and ingest popular movies from TMDB.
    
    This is the main periodic task for keeping movie data fresh.
    Configured to run via Celery Beat on a schedule.
    
    Args:
        pages: Number of pages to fetch (20 movies per page)
        region: Optional region filter (e.g., 'US')
    
    Returns:
        Dictionary with ingestion statistics:
        - log_id: Ingestion log UUID
        - movies_fetched: Total movies processed
        - movies_created: New movies added
        - movies_updated: Existing movies updated
        - errors: List of errors encountered
    
    Raises:
        TMDBAuthError: On authentication failure (not retried)
        TMDBAPIError: On API errors (retried with backoff)
        TMDBRateLimitError: On rate limit (retried after cooldown)
    
    Example:
        # Trigger manually
        ingest_popular_movies.delay(pages=10)
        
        # Or run synchronously (for testing)
        result = ingest_popular_movies(pages=1)
    """
    print("🔥 TASK STARTED 🔥", flush=True)
    # Create ingestion log
    log = IngestionLog.objects.create(
        job_type=IngestionLog.JobType.POPULAR_MOVIES,
        status=IngestionLog.Status.STARTED,
        metadata={
            'pages_requested': pages,
            'region': region,
            'task_id': self.request.id,
            'retry_count': self.request.retries,
        }
    )
    
    logger.info(
        f"Starting popular movies ingestion",
        extra={
            'log_id': str(log.id),
            'pages': pages,
            'region': region,
            'task_id': self.request.id,
        }
    )
    
    stats = {
        'movies_fetched': 0,
        'movies_created': 0,
        'movies_updated': 0,
        'errors': [],
    }
    
    try:
        with TMDBClient() as client:
            for page in range(1, pages + 1):
                try:
                    logger.info(f"Fetching page {page}/{pages}")
                    
                    # Fetch movies from TMDB
                    response = client.get_popular_movies(page=page, region=region)
                    movies_data = response.get('results', [])
                    
                    stats['movies_fetched'] += len(movies_data)
                    
                    # Process each movie
                    for movie_data in movies_data:
                        try:
                            # Enrich with full image URLs
                            if movie_data.get('poster_path'):
                                movie_data['poster_path'] = client.build_image_url(
                                    movie_data['poster_path'],
                                    size='w500'
                                )
                            
                            if movie_data.get('backdrop_path'):
                                movie_data['backdrop_path'] = client.build_image_url(
                                    movie_data['backdrop_path'],
                                    size='original'
                                )
                            
                            # Upsert movie (idempotent operation)
                            movie, created = MovieRepository.upsert_from_tmdb(movie_data)
                            
                            if created:
                                stats['movies_created'] += 1
                                logger.debug(f"Created movie: {movie.title} (TMDB ID: {movie.tmdb_id})")
                            else:
                                stats['movies_updated'] += 1
                                logger.debug(f"Updated movie: {movie.title} (TMDB ID: {movie.tmdb_id})")
                        
                        except IntegrityError as e:
                            error_msg = f"Integrity error for TMDB ID {movie_data.get('id')}: {str(e)}"
                            logger.warning(error_msg)
                            stats['errors'].append(error_msg)
                        
                        except Exception as e:
                            error_msg = f"Failed to process movie {movie_data.get('id', 'unknown')}: {str(e)}"
                            logger.error(error_msg, exc_info=True)
                            stats['errors'].append(error_msg)
                
                except TMDBRateLimitError as e:
                    logger.warning(
                        f"Rate limit hit on page {page}, will retry",
                        extra={'retry_after': e.retry_after}
                    )
                    # Re-raise to trigger Celery retry
                    raise
                
                except TMDBAPIError as e:
                    error_msg = f"API error on page {page}: {str(e)}"
                    logger.error(error_msg)
                    stats['errors'].append(error_msg)
                    # Continue to next page instead of failing entire job
                    continue
        
        # Mark log as completed
        log.status = (
            IngestionLog.Status.COMPLETED
            if not stats['errors']
            else IngestionLog.Status.PARTIAL
        )
        log.completed_at = timezone.now()
        log.movies_fetched = stats['movies_fetched']
        log.movies_created = stats['movies_created']
        log.movies_updated = stats['movies_updated']
        log.errors = stats['errors']
        log.save()
        
        logger.info(
            f"Completed popular movies ingestion",
            extra={
                'log_id': str(log.id),
                'movies_fetched': stats['movies_fetched'],
                'movies_created': stats['movies_created'],
                'movies_updated': stats['movies_updated'],
                'error_count': len(stats['errors']),
            }
        )
        
        return {
            'log_id': str(log.id),
            **stats,
        }
    
    except TMDBAuthError as e:
        # Authentication errors should not be retried
        logger.error(f"TMDB authentication failed: {str(e)}")
        
        log.status = IngestionLog.Status.FAILED
        log.completed_at = timezone.now()
        log.errors = [f"Authentication error: {str(e)}"]
        log.save()
        
        # Don't retry auth errors
        raise self.retry(exc=e, countdown=None, max_retries=0)
    
    except Exception as e:
        logger.error(
            f"Unexpected error in popular movies ingestion",
            exc_info=True,
            extra={'log_id': str(log.id)}
        )
        
        log.status = IngestionLog.Status.FAILED
        log.completed_at = timezone.now()
        log.errors = [f"Unexpected error: {str(e)}"]
        log.save()
        
        # Re-raise for Celery retry
        raise


@shared_task(
    bind=True,
    autoretry_for=(TMDBAPIError, TMDBRateLimitError),
    retry_backoff=True,
    retry_backoff_max=600,
    max_retries=3,
    retry_jitter=True,
)
def ingest_movie_detail(self, tmdb_id: int) -> Dict[str, Any]:
    """
    Fetch and ingest detailed information for a specific movie.
    
    Use this for enriching existing movies with complete data
    (genres, production companies, runtime, budget, etc.)
    
    Args:
        tmdb_id: TMDB movie ID
    
    Returns:
        Dictionary with:
        - movie_id: Django movie UUID
        - tmdb_id: TMDB ID
        - created: Whether movie was newly created
        - updated_fields: List of fields updated
    
    Example:
        # Enrich specific movie
        ingest_movie_detail.delay(tmdb_id=550)
    """
    logger.info(f"Fetching movie detail for TMDB ID: {tmdb_id}")
    
    try:
        with TMDBClient() as client:
            # Fetch detailed movie data
            movie_data = client.get_movie_detail(tmdb_id)
            
            # Enrich with full image URLs
            if movie_data.get('poster_path'):
                movie_data['poster_path'] = client.build_image_url(
                    movie_data['poster_path'],
                    size='w500'
                )
            
            if movie_data.get('backdrop_path'):
                movie_data['backdrop_path'] = client.build_image_url(
                    movie_data['backdrop_path'],
                    size='original'
                )
            
            # Upsert movie with transaction
            with transaction.atomic():
                movie, created = MovieRepository.upsert_from_tmdb(movie_data)
            
            logger.info(
                f"{'Created' if created else 'Updated'} movie: {movie.title}",
                extra={
                    'movie_id': str(movie.id),
                    'tmdb_id': tmdb_id,
                    'created': created,
                }
            )
            
            return {
                'movie_id': str(movie.id),
                'tmdb_id': tmdb_id,
                'title': movie.title,
                'created': created,
            }
    
    except TMDBNotFoundError as e:
        logger.warning(f"Movie not found in TMDB: {tmdb_id}")
        return {
            'tmdb_id': tmdb_id,
            'error': 'not_found',
            'message': str(e),
        }
    
    except Exception as e:
        logger.error(
            f"Failed to ingest movie detail for TMDB ID {tmdb_id}",
            exc_info=True
        )
        raise


@shared_task(
    bind=True,
    autoretry_for=(TMDBAPIError,),
    retry_backoff=True,
    max_retries=3,
)
def refresh_genres(self) -> Dict[str, Any]:
    """
    Sync TMDB genres to database.
    
    Should be run periodically (e.g., weekly) to keep genre list updated.
    
    Returns:
        Dictionary with:
        - genres_created: Number of new genres
        - genres_updated: Number of updated genres
        - total_genres: Total genres in database
    
    Example:
        # Run manually
        refresh_genres.delay()
    """
    logger.info("Starting genre sync from TMDB")
    
    stats = {
        'genres_created': 0,
        'genres_updated': 0,
    }
    
    try:
        with TMDBClient() as client:
            genres_data = client.get_genres()
            
            for genre_data in genres_data:
                with transaction.atomic():
                    genre, created = Genre.objects.update_or_create(
                        tmdb_id=genre_data['id'],
                        defaults={
                            'name': genre_data['name'],
                            'slug': genre_data['name'].lower().replace(' ', '-'),
                        }
                    )
                    
                    if created:
                        stats['genres_created'] += 1
                        logger.debug(f"Created genre: {genre.name}")
                    else:
                        stats['genres_updated'] += 1
                        logger.debug(f"Updated genre: {genre.name}")
        
        stats['total_genres'] = Genre.objects.count()
        
        logger.info(
            "Completed genre sync",
            extra={
                'genres_created': stats['genres_created'],
                'genres_updated': stats['genres_updated'],
                'total_genres': stats['total_genres'],
            }
        )
        
        return stats
    
    except Exception as e:
        logger.error("Failed to refresh genres", exc_info=True)
        raise


@shared_task(
    bind=True,
    autoretry_for=(TMDBAPIError,),
    retry_backoff=True,
    max_retries=3,
)
def ingest_trending_movies(self, time_window: str = 'day') -> Dict[str, Any]:
    """
    Fetch and ingest trending movies.
    
    Args:
        time_window: 'day' or 'week'
    
    Returns:
        Ingestion statistics
    
    Example:
        ingest_trending_movies.delay(time_window='week')
    """
    log = IngestionLog.objects.create(
        job_type=IngestionLog.JobType.TRENDING,
        status=IngestionLog.Status.STARTED,
        metadata={
            'time_window': time_window,
            'task_id': self.request.id,
        }
    )
    
    logger.info(f"Starting trending movies ingestion: {time_window}")
    
    stats = {
        'movies_fetched': 0,
        'movies_created': 0,
        'movies_updated': 0,
        'errors': [],
    }
    
    try:
        with TMDBClient() as client:
            response = client.get_trending_movies(time_window=time_window)
            movies_data = response.get('results', [])
            
            stats['movies_fetched'] = len(movies_data)
            
            for movie_data in movies_data:
                try:
                    # Enrich with full image URLs
                    if movie_data.get('poster_path'):
                        movie_data['poster_path'] = client.build_image_url(
                            movie_data['poster_path'],
                            size='w500'
                        )
                    
                    movie, created = MovieRepository.upsert_from_tmdb(movie_data)
                    
                    if created:
                        stats['movies_created'] += 1
                    else:
                        stats['movies_updated'] += 1
                
                except Exception as e:
                    error_msg = f"Failed to process movie {movie_data.get('id')}: {str(e)}"
                    logger.error(error_msg, exc_info=True)
                    stats['errors'].append(error_msg)
        
        log.status = (
            IngestionLog.Status.COMPLETED
            if not stats['errors']
            else IngestionLog.Status.PARTIAL
        )
        log.completed_at = timezone.now()
        log.movies_fetched = stats['movies_fetched']
        log.movies_created = stats['movies_created']
        log.movies_updated = stats['movies_updated']
        log.errors = stats['errors']
        log.save()
        
        logger.info(
            f"Completed trending movies ingestion",
            extra={'log_id': str(log.id), **stats}
        )
        
        return {'log_id': str(log.id), **stats}
    
    except Exception as e:
        log.status = IngestionLog.Status.FAILED
        log.completed_at = timezone.now()
        log.errors = [str(e)]
        log.save()
        
        logger.error("Failed trending movies ingestion", exc_info=True)
        raise


@shared_task(
    bind=True,
    autoretry_for=(TMDBAPIError,),
    retry_backoff=True,
    max_retries=3,
)
def batch_ingest_movies(self, tmdb_ids: List[int]) -> Dict[str, Any]:
    """
    Ingest multiple movies by TMDB ID in parallel.
    
    Uses Celery's group primitive for parallel execution.
    
    Args:
        tmdb_ids: List of TMDB movie IDs
    
    Returns:
        Dictionary with batch statistics
    
    Example:
        # Ingest specific movies
        batch_ingest_movies.delay([550, 551, 552])
    """
    logger.info(f"Starting batch ingestion of {len(tmdb_ids)} movies")
    
    # Create group of parallel tasks
    job = group(
        ingest_movie_detail.s(tmdb_id)
        for tmdb_id in tmdb_ids
    )
    
    # Execute tasks in parallel
    result = job.apply_async()
    
    # Wait for all tasks to complete (with timeout)
    results = result.get(timeout=300)  # 5 minute timeout
    
    # Aggregate statistics
    stats = {
        'total_movies': len(tmdb_ids),
        'successful': sum(1 for r in results if 'error' not in r),
        'failed': sum(1 for r in results if 'error' in r),
        'created': sum(1 for r in results if r.get('created')),
        'updated': sum(1 for r in results if not r.get('created') and 'error' not in r),
    }
    
    logger.info(
        f"Completed batch ingestion",
        extra=stats
    )
    
    return stats


@shared_task(
    bind=True,
    autoretry_for=(TMDBAPIError,),
    retry_backoff=True,
    max_retries=3,
)
def ingest_upcoming_movies(self, pages: int = 3) -> Dict[str, Any]:
    """
    Fetch and ingest upcoming movies.
    
    Args:
        pages: Number of pages to fetch
    
    Returns:
        Ingestion statistics
    """
    log = IngestionLog.objects.create(
        job_type=IngestionLog.JobType.UPCOMING,
        status=IngestionLog.Status.STARTED,
        metadata={
            'pages_requested': pages,
            'task_id': self.request.id,
        }
    )
    
    logger.info(f"Starting upcoming movies ingestion: {pages} pages")
    
    stats = {
        'movies_fetched': 0,
        'movies_created': 0,
        'movies_updated': 0,
        'errors': [],
    }
    
    try:
        with TMDBClient() as client:
            for page in range(1, pages + 1):
                try:
                    response = client.get_upcoming_movies(page=page)
                    movies_data = response.get('results', [])
                    
                    stats['movies_fetched'] += len(movies_data)
                    
                    for movie_data in movies_data:
                        try:
                            if movie_data.get('poster_path'):
                                movie_data['poster_path'] = client.build_image_url(
                                    movie_data['poster_path'],
                                    size='w500'
                                )
                            
                            movie, created = MovieRepository.upsert_from_tmdb(movie_data)
                            
                            if created:
                                stats['movies_created'] += 1
                            else:
                                stats['movies_updated'] += 1
                        
                        except Exception as e:
                            error_msg = f"Failed to process movie {movie_data.get('id')}: {str(e)}"
                            logger.error(error_msg, exc_info=True)
                            stats['errors'].append(error_msg)
                
                except TMDBRateLimitError:
                    raise  # Will be retried by Celery
                
                except TMDBAPIError as e:
                    error_msg = f"API error on page {page}: {str(e)}"
                    logger.error(error_msg)
                    stats['errors'].append(error_msg)
                    continue
        
        log.status = (
            IngestionLog.Status.COMPLETED
            if not stats['errors']
            else IngestionLog.Status.PARTIAL
        )
        log.completed_at = timezone.now()
        log.movies_fetched = stats['movies_fetched']
        log.movies_created = stats['movies_created']
        log.movies_updated = stats['movies_updated']
        log.errors = stats['errors']
        log.save()
        
        logger.info(
            f"Completed upcoming movies ingestion",
            extra={'log_id': str(log.id), **stats}
        )
        
        return {'log_id': str(log.id), **stats}
    
    except Exception as e:
        log.status = IngestionLog.Status.FAILED
        log.completed_at = timezone.now()
        log.errors = [str(e)]
        log.save()
        
        logger.error("Failed upcoming movies ingestion", exc_info=True)
        raise
