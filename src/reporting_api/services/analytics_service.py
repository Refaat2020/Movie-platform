"""
Movie analytics service using MongoDB aggregation pipelines.

This service encapsulates all MongoDB aggregation logic for reporting.
"""
from typing import List, Dict, Any, Optional
import logging
from datetime import datetime

from reporting_api.dependencies import get_movies_collection

logger = logging.getLogger(__name__)


class MovieAnalyticsService:
    """
    Service for movie analytics queries.
    
    Uses MongoDB aggregation pipelines for efficient data processing.
    """
    
    def __init__(self):
        self.collection = get_movies_collection()
    
    def get_highest_rated_movies(
        self,
        limit: int = 10,
        min_votes: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Get highest-rated movies using aggregation pipeline.
        
        Pipeline Strategy:
        - Filter: Only movies with sufficient votes (quality threshold)
        - Sort: By vote_average DESC, then vote_count DESC (tiebreaker)
        - Limit: Return top N
        - Project: Only return needed fields (performance)
        
        Indexes Used:
        - { vote_average: -1, vote_count: -1 } (compound index)
        
        Args:
            limit: Number of movies to return
            min_votes: Minimum vote count threshold
        
        Returns:
            List of movie documents
        """
        pipeline = [
            # Stage 1: Filter movies with enough votes
            {
                '$match': {
                    'vote_count': {'$gte': min_votes},
                    'vote_average': {'$gte': 0.0}  # Exclude unrated
                }
            },
            
            # Stage 2: Sort by rating (desc), then vote count (desc)
            {
                '$sort': {
                    'vote_average': -1,
                    'vote_count': -1
                }
            },
            
            # Stage 3: Limit results
            {'$limit': limit},
            
            # Stage 4: Project only needed fields
            {
                '$project': {
                    '_id': 0,
                    'title': 1,
                    'vote_average': 1,
                    'vote_count': 1,
                    'release_date': 1,
                    'poster_path': 1,
                }
            }
        ]
        
        logger.info(f"Executing highest-rated movies query: limit={limit}, min_votes={min_votes}")
        
        try:
            results = list(self.collection.aggregate(pipeline))
            logger.info(f"Found {len(results)} highest-rated movies")
            return results
        
        except Exception as e:
            logger.error(f"Aggregation pipeline failed: {e}", exc_info=True)
            raise
    
    def get_popular_movies_by_year(
        self,
        start_year: Optional[int] = None,
        end_year: Optional[int] = None,
        min_popularity: float = 0.0
    ) -> List[Dict[str, Any]]:
        """
        Get movies grouped by release year with statistics.
        
        Pipeline Strategy:
        - Filter: By year range and minimum popularity
        - AddFields: Extract year from release_date
        - Group: By year with aggregations (count, avg, sum)
        - Sort: By year descending
        
        Indexes Used:
        - { release_date: 1 } (for year filtering and sorting)
        
        Args:
            start_year: Filter from this year (inclusive)
            end_year: Filter to this year (inclusive)
            min_popularity: Minimum popularity threshold
        
        Returns:
            List of yearly statistics
        """
        pipeline = []
        
        # Stage 1: Build match criteria
        match_criteria = {
            'release_date': {'$ne': None},  # Exclude movies without release date
            'popularity': {'$gte': min_popularity}
        }
        
        # Add year range filter if specified
        if start_year or end_year:
            year_filter = {}
            if start_year:
                year_filter['$gte'] = datetime(start_year, 1, 1)
            if end_year:
                year_filter['$lte'] = datetime(end_year, 12, 31)
            
            if year_filter:
                match_criteria['release_date'] = year_filter
        
        pipeline.append({'$match': match_criteria})
        
        # Stage 2: Extract year from release_date
        pipeline.append({
            '$addFields': {
                'year': {'$year': '$release_date'}
            }
        })
        
        # Stage 3: Group by year with aggregations
        pipeline.append({
            '$group': {
                '_id': '$year',
                'count': {'$sum': 1},
                'avg_popularity': {'$avg': '$popularity'},
                'avg_rating': {'$avg': '$vote_average'},
                'total_votes': {'$sum': '$vote_count'}
            }
        })
        
        # Stage 4: Reshape output
        pipeline.append({
            '$project': {
                '_id': 0,
                'year': '$_id',
                'count': 1,
                'avg_popularity': 1,
                'avg_rating': 1,
                'total_votes': 1
            }
        })
        
        # Stage 5: Sort by year (most recent first)
        pipeline.append({'$sort': {'year': -1}})
        
        logger.info(
            f"Executing popular movies by year query: "
            f"start={start_year}, end={end_year}, min_pop={min_popularity}"
        )
        
        try:
            results = list(self.collection.aggregate(pipeline))
            logger.info(f"Found {len(results)} years with data")
            return results
        
        except Exception as e:
            logger.error(f"Aggregation pipeline failed: {e}", exc_info=True)
            raise
    
    def get_genre_popularity(
        self,
        limit: int = 20,
        sort_by: str = 'popularity'
    ) -> List[Dict[str, Any]]:
        """
        Get genre popularity rankings.
        
        Pipeline Strategy:
        - Unwind: Split movies with multiple genres into separate documents
        - Group: By genre with aggregations
        - Sort: By specified metric
        - Limit: Top N genres
        
        Indexes Used:
        - { "genres.slug": 1 } (multi-key index on array field)
        
        Args:
            limit: Number of genres to return
            sort_by: Sort metric (popularity, count, or rating)
        
        Returns:
            List of genre statistics
        """
        # Map sort_by to aggregation field
        sort_field_map = {
            'popularity': 'avg_popularity',
            'count': 'count',
            'rating': 'avg_rating'
        }
        sort_field = sort_field_map.get(sort_by, 'avg_popularity')
        
        pipeline = [
            # Stage 1: Filter movies with genres
            {
                '$match': {
                    'genres': {'$exists': True, '$ne': []}
                }
            },
            
            # Stage 2: Unwind genres array
            {'$unwind': '$genres'},
            
            # Stage 3: Group by genre
            {
                '$group': {
                    '_id': '$genres.name',
                    'count': {'$sum': 1},
                    'avg_popularity': {'$avg': '$popularity'},
                    'avg_rating': {'$avg': '$vote_average'}
                }
            },
            
            # Stage 4: Reshape output
            {
                '$project': {
                    '_id': 0,
                    'genre': '$_id',
                    'count': 1,
                    'avg_popularity': 1,
                    'avg_rating': 1
                }
            },
            
            # Stage 5: Sort by specified metric
            {'$sort': {sort_field: -1}},
            
            # Stage 6: Limit results
            {'$limit': limit}
        ]
        
        logger.info(f"Executing genre popularity query: limit={limit}, sort_by={sort_by}")
        
        try:
            results = list(self.collection.aggregate(pipeline))
            logger.info(f"Found {len(results)} genres")
            return results
        
        except Exception as e:
            logger.error(f"Aggregation pipeline failed: {e}", exc_info=True)
            raise
    
    def get_movies_by_status(self) -> List[Dict[str, Any]]:
        """
        Get movie count grouped by status.
        
        Pipeline Strategy:
        - Group: By status field
        - Count: Movies per status
        - Sort: By count descending
        
        Returns:
            List of status counts
        """
        pipeline = [
            # Stage 1: Group by status
            {
                '$group': {
                    '_id': '$status',
                    'count': {'$sum': 1}
                }
            },
            
            # Stage 2: Reshape output
            {
                '$project': {
                    '_id': 0,
                    'status': '$_id',
                    'count': 1
                }
            },
            
            # Stage 3: Sort by count
            {'$sort': {'count': -1}}
        ]
        
        logger.info("Executing movies by status query")
        
        try:
            results = list(self.collection.aggregate(pipeline))
            logger.info(f"Found {len(results)} statuses")
            return results
        
        except Exception as e:
            logger.error(f"Aggregation pipeline failed: {e}", exc_info=True)
            raise
