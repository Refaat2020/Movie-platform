"""
MongoDB sync helper functions and utilities.

This module provides:
- High-level sync orchestration
- Sync validation and verification
- Bulk sync utilities
- Sync state management
- Recovery helpers

Difference from tasks.py:
- tasks.py: Celery tasks (async, retryable)
- sync.py: Utility functions (can be used synchronously or called by tasks)
"""
import logging
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta
from django.db.models import Q, F, Count
from django.utils import timezone

from src.apps.movies.models import Movie, Review
from src.apps.integrations.mongodb.client import mongodb_client
from src.apps.integrations.mongodb.repository import (
    MongoMovieRepository,
    MongoReviewRepository,
)

logger = logging.getLogger(__name__)


class SyncOrchestrator:
    """
    High-level orchestrator for MongoDB sync operations.
    
    Provides coordinated sync logic that can be used by:
    - Celery tasks
    - Management commands
    - API endpoints
    - Tests
    """
    
    def __init__(self):
        self.movie_repo = MongoMovieRepository()
        self.review_repo = MongoReviewRepository()
    
    def sync_movie(self, movie: Movie, force: bool = False) -> Dict[str, Any]:
        """
        Sync a single movie to MongoDB (synchronous).
        
        Args:
            movie: Django Movie instance
            force: Force sync even if already synced
        
        Returns:
            Sync result dictionary
        
        Usage:
            orchestrator = SyncOrchestrator()
            result = orchestrator.sync_movie(movie)
        """
        # Check if sync needed (unless forced)
        if not force and self._is_movie_synced(movie):
            logger.debug(f"Movie {movie.id} already synced, skipping")
            return {
                'movie_id': str(movie.id),
                'synced': False,
                'reason': 'already_synced',
            }
        
        try:
            # Transform and sync
            movie_doc = MongoMovieRepository.transform_movie_to_document(movie)
            self.movie_repo.upsert_movie(movie_doc)
            
            # Update sync timestamp
            movie.mark_synced()
            
            return {
                'movie_id': str(movie.id),
                'title': movie.title,
                'synced': True,
            }
        
        except Exception as e:
            logger.error(f"Failed to sync movie {movie.id}: {e}", exc_info=True)
            return {
                'movie_id': str(movie.id),
                'synced': False,
                'error': str(e),
            }
    
    def sync_review(self, review: Review) -> Dict[str, Any]:
        """
        Sync a review and its parent movie (synchronous).
        
        Args:
            review: Django Review instance
        
        Returns:
            Sync result dictionary
        """
        try:
            # Sync review document
            review_doc = MongoReviewRepository.transform_review_to_document(review)
            self.review_repo.upsert_review(review_doc)
            
            # Re-sync parent movie to update review_stats
            self.sync_movie(review.movie, force=True)
            
            return {
                'review_id': str(review.id),
                'movie_id': str(review.movie.id),
                'synced': True,
            }
        
        except Exception as e:
            logger.error(f"Failed to sync review {review.id}: {e}", exc_info=True)
            return {
                'review_id': str(review.id),
                'synced': False,
                'error': str(e),
            }
    
    def bulk_sync_movies(
        self,
        movie_ids: List[str],
        batch_size: int = 100
    ) -> Dict[str, Any]:
        """
        Bulk sync movies with batching (synchronous).
        
        More efficient than individual syncs for large datasets.
        
        Args:
            movie_ids: List of movie UUIDs
            batch_size: Number of movies per batch
        
        Returns:
            Statistics dictionary
        """
        stats = {
            'total': len(movie_ids),
            'synced': 0,
            'failed': 0,
            'errors': [],
        }
        
        # Process in batches
        for i in range(0, len(movie_ids), batch_size):
            batch_ids = movie_ids[i:i + batch_size]
            
            try:
                # Fetch movies with prefetch
                movies = Movie.objects.filter(
                    id__in=batch_ids
                ).select_related().prefetch_related(
                    'genres',
                    # 'production_companies',
                    'reviews'
                )
                
                # Transform all movies
                movies_data = [
                    MongoMovieRepository.transform_movie_to_document(movie)
                    for movie in movies
                ]
                
                # Bulk upsert to MongoDB
                result = self.movie_repo.bulk_upsert_movies(movies_data)
                stats['synced'] += result['upserted'] + result['modified']
                
                # Update sync timestamps in batch
                Movie.objects.filter(id__in=batch_ids).update(
                    synced_to_mongo_at=timezone.now()
                )
            
            except Exception as e:
                logger.error(f"Batch sync failed: {e}", exc_info=True)
                stats['failed'] += len(batch_ids)
                stats['errors'].append(str(e))
        
        logger.info(
            f"Bulk sync complete: {stats['synced']}/{stats['total']} synced, "
            f"{stats['failed']} failed"
        )
        
        return stats
    
    def _is_movie_synced(self, movie: Movie) -> bool:
        """
        Check if movie is already synced and up-to-date.
        
        A movie is considered synced if:
        - synced_to_mongo_at is not None
        - synced_to_mongo_at >= updated_at
        """
        if not movie.synced_to_mongo_at:
            return False
        
        return movie.synced_to_mongo_at >= movie.updated_at


class SyncValidator:
    """
    Validates sync integrity between PostgreSQL and MongoDB.
    
    Useful for:
    - Health checks
    - Debugging sync issues
    - Recovery verification
    """
    
    def __init__(self):
        self.movie_collection = mongodb_client.get_collection('movies')
        self.review_collection = mongodb_client.get_collection('reviews')
    
    def validate_movie_sync(self, movie_id: str) -> Dict[str, Any]:
        """
        Validate that a movie is correctly synced.
        
        Checks:
        - Movie exists in both databases
        - Key fields match
        - Sync timestamp is reasonable
        
        Args:
            movie_id: Django Movie UUID
        
        Returns:
            Validation result with issues (if any)
        """
        issues = []
        
        # Check PostgreSQL
        try:
            pg_movie = Movie.objects.get(id=movie_id)
        except Movie.DoesNotExist:
            return {
                'movie_id': movie_id,
                'valid': False,
                'issues': ['Movie not found in PostgreSQL'],
            }
        
        # Check MongoDB
        mongo_doc = self.movie_collection.find_one({'movie_id': movie_id})
        if not mongo_doc:
            issues.append('Movie not found in MongoDB')
            return {
                'movie_id': movie_id,
                'valid': False,
                'issues': issues,
            }
        
        # Validate key fields match
        if pg_movie.title != mongo_doc.get('title'):
            issues.append(f"Title mismatch: PG='{pg_movie.title}' vs Mongo='{mongo_doc.get('title')}'")
        
        if float(pg_movie.popularity) != mongo_doc.get('popularity', 0.0):
            issues.append(f"Popularity mismatch: PG={pg_movie.popularity} vs Mongo={mongo_doc.get('popularity')}")
        
        # Check sync freshness
        if pg_movie.synced_to_mongo_at:
            if pg_movie.synced_to_mongo_at < pg_movie.updated_at:
                issues.append('Sync timestamp older than update timestamp (stale)')
        else:
            issues.append('No sync timestamp in PostgreSQL')
        
        return {
            'movie_id': movie_id,
            'valid': len(issues) == 0,
            'issues': issues,
        }
    
    def find_sync_discrepancies(self, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Find movies with sync discrepancies.
        
        Returns list of movies that:
        - Exist in PostgreSQL but not MongoDB
        - Have stale sync timestamps
        - Have mismatched data
        
        Args:
            limit: Maximum discrepancies to return
        
        Returns:
            List of discrepancy reports
        """
        discrepancies = []
        
        # Find movies never synced or stale
        stale_movies = Movie.objects.filter(
            Q(synced_to_mongo_at__isnull=True) |
            Q(synced_to_mongo_at__lt=F('updated_at')),
            is_active=True
        )[:limit]
        
        for movie in stale_movies:
            validation = self.validate_movie_sync(str(movie.id))
            if not validation['valid']:
                discrepancies.append(validation)
        
        return discrepancies
    
    def get_sync_statistics(self) -> Dict[str, Any]:
        """
        Get comprehensive sync statistics.
        
        Returns:
            Dictionary with sync metrics
        """
        # PostgreSQL stats
        total_pg_movies = Movie.objects.filter(is_active=True).count()
        never_synced = Movie.objects.filter(
            is_active=True,
            synced_to_mongo_at__isnull=True
        ).count()
        stale_synced = Movie.objects.filter(
            is_active=True,
            synced_to_mongo_at__isnull=False,
            synced_to_mongo_at__lt=F('updated_at')
        ).count()
        
        # MongoDB stats
        total_mongo_movies = self.movie_collection.count_documents({})
        total_mongo_reviews = self.review_collection.count_documents({})
        
        # Sync lag (movies not synced in last 5 minutes)
        five_min_ago = timezone.now() - timedelta(minutes=5)
        recent_pg_updates = Movie.objects.filter(
            is_active=True,
            updated_at__gte=five_min_ago
        ).count()
        
        return {
            'postgres': {
                'total_movies': total_pg_movies,
                'never_synced': never_synced,
                'stale_synced': stale_synced,
                'recent_updates': recent_pg_updates,
            },
            'mongodb': {
                'total_movies': total_mongo_movies,
                'total_reviews': total_mongo_reviews,
            },
            'sync_health': {
                'health_score': self._calculate_health_score(
                    total_pg_movies,
                    never_synced,
                    stale_synced
                ),
                'status': self._get_health_status(never_synced, stale_synced),
            }
        }
    
    def _calculate_health_score(
        self,
        total: int,
        never_synced: int,
        stale: int
    ) -> float:
        """Calculate sync health score (0-100)."""
        if total == 0:
            return 100.0
        
        synced_correctly = total - never_synced - stale
        return (synced_correctly / total) * 100
    
    def _get_health_status(self, never_synced: int, stale: int) -> str:
        """Get health status label."""
        total_issues = never_synced + stale
        
        if total_issues == 0:
            return 'excellent'
        elif total_issues < 10:
            return 'good'
        elif total_issues < 50:
            return 'degraded'
        else:
            return 'poor'


class SyncRecovery:
    """
    Recovery utilities for sync failures.
    
    Provides tools for:
    - Identifying missing/stale documents
    - Re-syncing failed operations
    - Cleaning up orphaned documents
    """
    
    def __init__(self):
        self.orchestrator = SyncOrchestrator()
        self.validator = SyncValidator()
        self.movie_collection = mongodb_client.get_collection('movies')
    
    def recover_missing_documents(self, limit: int = 1000) -> Dict[str, Any]:
        """
        Find and sync movies missing from MongoDB.
        
        Args:
            limit: Maximum movies to recover
        
        Returns:
            Recovery statistics
        """
        logger.info(f"Starting recovery of missing documents (limit: {limit})")
        
        # Find movies never synced
        missing_movies = Movie.objects.filter(
            is_active=True,
            synced_to_mongo_at__isnull=True
        )[:limit]
        
        movie_ids = [str(m.id) for m in missing_movies]
        
        if not movie_ids:
            logger.info("No missing documents found")
            return {'recovered': 0, 'total': 0}
        
        # Bulk sync
        result = self.orchestrator.bulk_sync_movies(movie_ids)
        
        logger.info(f"Recovery complete: {result}")
        return result
    
    def recover_stale_documents(self, limit: int = 1000) -> Dict[str, Any]:
        """
        Re-sync movies with stale sync timestamps.
        
        Args:
            limit: Maximum movies to recover
        
        Returns:
            Recovery statistics
        """
        logger.info(f"Starting recovery of stale documents (limit: {limit})")
        
        # Find stale movies
        stale_movies = Movie.objects.filter(
            is_active=True,
            synced_to_mongo_at__isnull=False,
            synced_to_mongo_at__lt=F('updated_at')
        )[:limit]
        
        movie_ids = [str(m.id) for m in stale_movies]
        
        if not movie_ids:
            logger.info("No stale documents found")
            return {'recovered': 0, 'total': 0}
        
        # Bulk sync
        result = self.orchestrator.bulk_sync_movies(movie_ids)
        
        logger.info(f"Recovery complete: {result}")
        return result
    
    def cleanup_orphaned_documents(self) -> Dict[str, int]:
        """
        Remove MongoDB documents that don't exist in PostgreSQL.
        
        This can happen if:
        - Movie deleted from PostgreSQL but sync failed
        - Manual database manipulation
        
        Returns:
            Count of orphaned documents removed
        """
        logger.info("Starting cleanup of orphaned documents")
        
        # Get all movie IDs from MongoDB
        mongo_ids = set(
            doc['movie_id']
            for doc in self.movie_collection.find({}, {'movie_id': 1})
        )
        
        # Get all movie IDs from PostgreSQL
        pg_ids = set(
            str(id)
            for id in Movie.objects.filter(is_active=True).values_list('id', flat=True)
        )
        
        # Find orphaned IDs (in MongoDB but not PostgreSQL)
        orphaned_ids = mongo_ids - pg_ids
        
        if not orphaned_ids:
            logger.info("No orphaned documents found")
            return {'deleted': 0}
        
        # Delete orphaned documents
        result = self.movie_collection.delete_many({
            'movie_id': {'$in': list(orphaned_ids)}
        })
        
        logger.info(f"Cleaned up {result.deleted_count} orphaned documents")
        
        return {'deleted': result.deleted_count}


# Convenience functions for common operations

def quick_sync_movie(movie_id: str, force: bool = False) -> Dict[str, Any]:
    """
    Quick synchronous movie sync.
    
    Usage:
        from apps.integrations.mongodb.sync import quick_sync_movie
        result = quick_sync_movie('abc-123')
    """
    movie = Movie.objects.select_related().prefetch_related(
        'genres', 'reviews'
    ).get(id=movie_id)
    
    orchestrator = SyncOrchestrator()
    return orchestrator.sync_movie(movie, force=force)


def validate_sync_integrity(movie_id: str) -> Dict[str, Any]:
    """
    Quick sync validation.
    
    Usage:
        from apps.integrations.mongodb.sync import validate_sync_integrity
        result = validate_sync_integrity('abc-123')
        if not result['valid']:
            print(result['issues'])
    """
    validator = SyncValidator()
    return validator.validate_movie_sync(movie_id)


def get_sync_health() -> Dict[str, Any]:
    """
    Quick health check.
    
    Usage:
        from apps.integrations.mongodb.sync import get_sync_health
        health = get_sync_health()
        print(f"Status: {health['sync_health']['status']}")
    """
    validator = SyncValidator()
    return validator.get_sync_statistics()