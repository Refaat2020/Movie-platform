"""
MongoDB repository for movie sync operations.

Field Selection Strategy:
--------------------------
We sync only fields useful for analytics and reporting to minimize:
- Storage costs
- Sync time
- Network bandwidth

INCLUDED fields (analytics-relevant):
- movie_id, tmdb_id: Identifiers for cross-referencing
- title, overview: For search and display
- release_date, runtime: Temporal and duration analytics
- popularity, vote_average, vote_count: Ranking and quality metrics
- genres, production_companies: Categorical analysis (denormalized)
- status, source: Filtering dimensions
- review_stats: Pre-aggregated review metrics (denormalized)
- poster_path: For UI display in reporting dashboards
- created_at, updated_at, synced_at: Temporal tracking

EXCLUDED fields (not needed for reporting):
- tagline: Not used in analytics queries
- budget, revenue: Financial data (could add later if needed)
- backdrop_path: Secondary image (saves storage)
- is_active: Handled via deletion instead

Index Strategy:
--------------
Indexes optimized for common reporting queries:
1. movie_id (unique): Primary lookup
2. popularity + release_date: "Top movies this year"
3. vote_average + vote_count: "Highest rated"
4. genres.slug: "Movies by genre"
5. release_date: Temporal queries
6. text(title, overview): Full-text search
7. synced_at: Monitoring sync lag
"""
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime
from decimal import Decimal

from src.apps.integrations.mongodb.client import mongodb_client

logger = logging.getLogger(__name__)
from datetime import datetime, date, time
from django.utils.timezone import make_aware

class MongoMovieRepository:
    """
    Repository for syncing movies to MongoDB.
    
    Responsibilities:
    - Transform Django model instances to MongoDB documents
    - Upsert operations (idempotent)
    - Delete operations
    - Bulk operations for efficiency
    """
    
    def __init__(self):
        self.collection = mongodb_client.get_collection('movies')
    
    def upsert_movie(self, movie_data: Dict[str, Any]) -> bool:
        """
        Upsert a movie document to MongoDB.
        
        Args:
            movie_data: Dictionary with movie fields
        
        Returns:
            True if successful
        
        Document Structure:
        {
            "movie_id": "uuid",           # Django primary key
            "tmdb_id": 550,               # TMDB ID (nullable)
            "source": "external",         # external/internal
            "title": "Fight Club",
            "overview": "...",
            "release_date": ISODate("1999-10-15"),
            "runtime": 139,
            "popularity": 85.234,
            "vote_average": 8.4,
            "vote_count": 25000,
            "poster_path": "https://...",
            "status": "released",
            
            # Denormalized arrays (for filtering without joins)
            "genres": [
                {"name": "Drama", "slug": "drama"},
                {"name": "Thriller", "slug": "thriller"}
            ],
            "production_companies": [
                {"name": "20th Century Fox", "country": "US"}
            ],
            
            # Pre-aggregated review statistics
            "review_stats": {
                "count": 150,
                "average_rating": 7.8,
                "rating_distribution": {
                    "1-2": 5,
                    "3-4": 10,
                    "5-6": 25,
                    "7-8": 60,
                    "9-10": 50
                }
            },
            
            # Timestamps
            "created_at": ISODate("2024-01-01T00:00:00Z"),
            "updated_at": ISODate("2024-01-05T12:30:00Z"),
            "synced_at": ISODate("2024-01-05T12:30:01Z")
        }
        """
        try:
            # Add sync timestamp
            movie_data['synced_at'] = datetime.utcnow()
            
            # Upsert using movie_id as unique key
            result = self.collection.update_one(
                {'movie_id': movie_data['movie_id']},
                {'$set': movie_data},
                upsert=True
            )
            
            if result.upserted_id:
                logger.info(f"Created MongoDB document for movie {movie_data['movie_id']}")
            else:
                logger.info(f"Updated MongoDB document for movie {movie_data['movie_id']}")
            
            return True
        
        except Exception as e:
            logger.error(
                f"Failed to upsert movie {movie_data.get('movie_id')} to MongoDB",
                exc_info=True
            )
            raise
    
    def delete_movie(self, movie_id: str) -> bool:
        """
        Delete a movie document from MongoDB.
        
        Args:
            movie_id: Django movie UUID
        
        Returns:
            True if deleted, False if not found
        """
        try:
            result = self.collection.delete_one({'movie_id': movie_id})
            
            if result.deleted_count > 0:
                logger.info(f"Deleted MongoDB document for movie {movie_id}")
                return True
            else:
                logger.warning(f"No MongoDB document found for movie {movie_id}")
                return False
        
        except Exception as e:
            logger.error(
                f"Failed to delete movie {movie_id} from MongoDB",
                exc_info=True
            )
            raise
    
    def bulk_upsert_movies(self, movies_data: List[Dict[str, Any]]) -> Dict[str, int]:
        """
        Bulk upsert movies for efficiency.
        
        Args:
            movies_data: List of movie dictionaries
        
        Returns:
            Statistics: {'upserted': N, 'modified': M}
        """
        if not movies_data:
            return {'upserted': 0, 'modified': 0}
        
        try:
            from pymongo import UpdateOne
            
            # Prepare bulk operations
            operations = []
            for movie_data in movies_data:
                movie_data['synced_at'] = datetime.utcnow()
                
                operations.append(
                    UpdateOne(
                        {'movie_id': movie_data['movie_id']},
                        {'$set': movie_data},
                        upsert=True
                    )
                )
            
            # Execute bulk write
            result = self.collection.bulk_write(operations, ordered=False)
            
            stats = {
                'upserted': result.upserted_count,
                'modified': result.modified_count,
            }
            
            logger.info(
                f"Bulk synced {len(movies_data)} movies to MongoDB: "
                f"{stats['upserted']} created, {stats['modified']} updated"
            )
            
            return stats
        
        except Exception as e:
            logger.error("Failed to bulk upsert movies to MongoDB", exc_info=True)
            raise
    
    def get_sync_lag_count(self, minutes: int = 5) -> int:
        """
        Count movies not synced in last N minutes.
        
        Used for monitoring sync health.
        
        Args:
            minutes: Threshold in minutes
        
        Returns:
            Count of stale documents
        """
        from datetime import timedelta
        
        threshold = datetime.utcnow() - timedelta(minutes=minutes)
        
        count = self.collection.count_documents({
            '$or': [
                {'synced_at': {'$lt': threshold}},
                {'synced_at': {'$exists': False}}
            ]
        })
        
        return count
    @staticmethod
    def serialize_datetime(value):
        if value is None:
            return None

        if isinstance(value, datetime):
            return value

        if isinstance(value, date):
            return make_aware(datetime.combine(value, time.min))

        return value
    
    @staticmethod
    def transform_movie_to_document(movie) -> Dict[str, Any]:
        """
        Transform Django Movie model instance to MongoDB document.
        
        This is where we select which fields to sync.
        
        Args:
            movie: Django Movie model instance
        
        Returns:
            Dictionary ready for MongoDB insertion
        """
        # Base fields
        doc = {
            'movie_id': str(movie.id),
            'tmdb_id': movie.tmdb_id,
            'source': movie.source,
            'title': movie.title,
            'overview': movie.overview,
            'release_date':  MongoMovieRepository.serialize_datetime(movie.release_date), 
            'runtime': movie.runtime,
            'popularity': float(movie.popularity) if movie.popularity else 0.0,
            'vote_average': float(movie.vote_average) if movie.vote_average else 0.0,
            'vote_count': movie.vote_count,
            'poster_path': movie.poster_path,
            'status': movie.status,
            'created_at':  MongoMovieRepository.serialize_datetime(movie.created_at),
            'updated_at':  MongoMovieRepository.serialize_datetime(movie.updated_at),
        }
        
        # Denormalized genres (for filtering without joins)
        # We store both name and slug for flexibility
        doc['genres'] = [
            {
                'name': genre.name,
                'slug': genre.slug,
            }
            for genre in movie.genres.all()
        ]
        
        # Denormalized production companies
        # We only need name and country for analytics
        # doc['production_companies'] = [
        #     {
        #         'name': company.name,
        #         'country': company.origin_country,
        #     }
        #     for company in movie.production_companies.all()
        # ]
        
        # Pre-aggregate review statistics
        # This avoids expensive joins in reporting queries
        doc['review_stats'] = MongoMovieRepository.aggregate_review_stats(movie)
        
        return doc
    
    @staticmethod
    def aggregate_review_stats(movie) -> Dict[str, Any]:
        """
        Pre-aggregate review statistics for a movie.
        
        Why pre-aggregate?
        - MongoDB reporting queries are much faster with denormalized stats
        - Avoids expensive aggregation pipelines
        - Review stats change infrequently relative to query frequency
        
        Args:
            movie: Django Movie model instance
        
        Returns:
            Dictionary with review statistics
        """
        from django.db.models import Avg, Count, Q
        
        reviews = movie.reviews.all()
        
        stats = {
            'count': reviews.count(),
            'average_rating': 0.0,
            'rating_distribution': {
                '1-2': 0,
                '3-4': 0,
                '5-6': 0,
                '7-8': 0,
                '9-10': 0,
            }
        }
        
        # Average rating
        avg_result = reviews.aggregate(avg_rating=Avg('rating'))
        if avg_result['avg_rating']:
            stats['average_rating'] = float(avg_result['avg_rating'])
        
        # Rating distribution (for histograms)
        # Group ratings into buckets for visualizations
        stats['rating_distribution']['1-2'] = reviews.filter(
            rating__gte=0.0, rating__lt=3.0
        ).count()
        
        stats['rating_distribution']['3-4'] = reviews.filter(
            rating__gte=3.0, rating__lt=5.0
        ).count()
        
        stats['rating_distribution']['5-6'] = reviews.filter(
            rating__gte=5.0, rating__lt=7.0
        ).count()
        
        stats['rating_distribution']['7-8'] = reviews.filter(
            rating__gte=7.0, rating__lt=9.0
        ).count()
        
        stats['rating_distribution']['9-10'] = reviews.filter(
            rating__gte=9.0, rating__lte=10.0
        ).count()
        
        return stats


class MongoReviewRepository:
    """
    Repository for syncing reviews to MongoDB.
    
    Review documents are separate from movies for:
    - Efficient pagination of movie reviews
    - Independent review queries (e.g., user's reviews)
    - Smaller document size (MongoDB 16MB limit)
    """
    
    def __init__(self):
        self.collection = mongodb_client.get_collection('reviews')
    
    def upsert_review(self, review_data: Dict[str, Any]) -> bool:
        """
        Upsert a review document to MongoDB.
        
        Document Structure:
        {
            "review_id": "uuid",
            "movie_id": "uuid",          # For querying movie reviews
            "movie_title": "Fight Club", # Denormalized for display
            "user_id": 123,
            "rating": 8.5,
            "title": "Amazing film!",
            "content": "...",
            "is_spoiler": false,
            "helpful_count": 42,
            "created_at": ISODate("2024-01-01T00:00:00Z"),
            "updated_at": ISODate("2024-01-05T12:30:00Z")
        }
        """
        try:
            result = self.collection.update_one(
                {'review_id': review_data['review_id']},
                {'$set': review_data},
                upsert=True
            )
            
            if result.upserted_id:
                logger.info(f"Created MongoDB document for review {review_data['review_id']}")
            else:
                logger.info(f"Updated MongoDB document for review {review_data['review_id']}")
            
            return True
        
        except Exception as e:
            logger.error(
                f"Failed to upsert review {review_data.get('review_id')} to MongoDB",
                exc_info=True
            )
            raise
    
    def delete_review(self, review_id: str) -> bool:
        """Delete a review document from MongoDB."""
        try:
            result = self.collection.delete_one({'review_id': review_id})
            
            if result.deleted_count > 0:
                logger.info(f"Deleted MongoDB document for review {review_id}")
                return True
            else:
                logger.warning(f"No MongoDB document found for review {review_id}")
                return False
        
        except Exception as e:
            logger.error(
                f"Failed to delete review {review_id} from MongoDB",
                exc_info=True
            )
            raise
    
    @staticmethod
    def transform_review_to_document(review) -> Dict[str, Any]:
        """
        Transform Django Review model instance to MongoDB document.
        
        Args:
            review: Django Review model instance
        
        Returns:
            Dictionary ready for MongoDB insertion
        """
        return {
            'review_id': str(review.id),
            'movie_id': str(review.movie.id),
            'movie_title': review.movie.title,  # Denormalized
            'user_id': review.user.id,
            'rating': float(review.rating),
            'title': review.title,
            'content': review.content,
            'is_spoiler': review.is_spoiler,
            'helpful_count': review.helpful_count,
            'created_at': review.created_at,
            'updated_at': review.updated_at,
        }
