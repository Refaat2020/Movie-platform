"""
Movie reporting endpoints.

Implements MongoDB aggregation pipelines for analytics queries.
"""
from fastapi import APIRouter, HTTPException, Query
from typing import Optional
import logging

from reporting_api.dependencies import get_movies_collection
from reporting_api.schemas.movies import (
    HighestRatedMoviesResponse,
    HighestRatedMovie,
    PopularMoviesSummaryResponse,
    PopularMoviesByYear,
    GenrePopularityResponse,
    GenrePopularity,
)
from reporting_api.services.analytics_service import MovieAnalyticsService

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get(
    "/highest-rated-movies",
    response_model=HighestRatedMoviesResponse,
    summary="Get highest-rated movies",
    description="Returns top-rated movies sorted by vote average and vote count",
)
async def get_highest_rated_movies(
    limit: int = Query(10, ge=1, le=100, description="Number of movies to return"),
    min_votes: int = Query(100, ge=0, description="Minimum vote count threshold"),
) -> HighestRatedMoviesResponse:
    """
    Get highest-rated movies using MongoDB aggregation.
    
    Pipeline:
    1. Filter movies with minimum vote count (quality threshold)
    2. Sort by vote_average (desc), then vote_count (desc)
    3. Limit results
    4. Project only needed fields
    
    Args:
        limit: Number of movies to return (1-100)
        min_votes: Minimum votes required (prevents low-sample bias)
    
    Returns:
        HighestRatedMoviesResponse with top-rated movies
    
    Example:
        GET /report/highest-rated-movies?limit=10&min_votes=1000
    """
    try:
        service = MovieAnalyticsService()
        movies = service.get_highest_rated_movies(limit=limit, min_votes=min_votes)
        
        return HighestRatedMoviesResponse(
            movies=[
                HighestRatedMovie(
                    title=m['title'],
                    vote_average=m['vote_average'],
                    vote_count=m['vote_count'],
                    release_date=m.get('release_date'),
                    poster_path=m.get('poster_path'),
                )
                for m in movies
            ],
            total=len(movies)
        )
    
    except Exception as e:
        logger.error(f"Error fetching highest-rated movies: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail={
                "code": "QUERY_FAILED",
                "message": "Failed to fetch highest-rated movies"
            }
        )


@router.get(
    "/popular-movies-summary",
    response_model=PopularMoviesSummaryResponse,
    summary="Get popular movies summary by year",
    description="Aggregates movies by release year with statistics",
)
async def get_popular_movies_summary(
    start_year: Optional[int] = Query(None, ge=1900, le=2100, description="Start year (inclusive)"),
    end_year: Optional[int] = Query(None, ge=1900, le=2100, description="End year (inclusive)"),
    min_popularity: float = Query(0.0, ge=0.0, description="Minimum popularity threshold"),
) -> PopularMoviesSummaryResponse:
    """
    Get popular movies summary grouped by year.
    
    Pipeline:
    1. Filter by year range (if specified)
    2. Filter by minimum popularity
    3. Extract year from release_date
    4. Group by year with aggregations:
       - count: Number of movies
       - avg_popularity: Average popularity score
       - avg_rating: Average vote average
       - total_votes: Sum of vote counts
    5. Sort by year (descending)
    
    Args:
        start_year: Filter movies from this year onwards
        end_year: Filter movies up to this year
        min_popularity: Only include movies above this popularity
    
    Returns:
        PopularMoviesSummaryResponse with yearly statistics
    
    Example:
        GET /report/popular-movies-summary?start_year=2020&end_year=2024
    """
    try:
        service = MovieAnalyticsService()
        summary = service.get_popular_movies_by_year(
            start_year=start_year,
            end_year=end_year,
            min_popularity=min_popularity
        )
        
        return PopularMoviesSummaryResponse(
            summary=[
                PopularMoviesByYear(
                    year=s['year'],
                    count=s['count'],
                    avg_popularity=round(s['avg_popularity'], 2),
                    avg_rating=round(s['avg_rating'], 2),
                    total_votes=s['total_votes'],
                )
                for s in summary
            ],
            total_years=len(summary)
        )
    
    except Exception as e:
        logger.error(f"Error fetching popular movies summary: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail={
                "code": "QUERY_FAILED",
                "message": "Failed to fetch popular movies summary"
            }
        )


@router.get(
    "/genre-popularity",
    response_model=GenrePopularityResponse,
    summary="Get genre popularity rankings",
    description="Ranks genres by number of movies and average popularity",
)
async def get_genre_popularity(
    limit: int = Query(20, ge=1, le=50, description="Number of genres to return"),
    sort_by: str = Query("popularity", regex="^(popularity|count|rating)$", description="Sort by: popularity, count, or rating"),
) -> GenrePopularityResponse:
    """
    Get genre popularity rankings.
    
    Pipeline:
    1. Unwind genres array (one document per genre)
    2. Group by genre name with aggregations
    3. Sort by specified metric
    4. Limit results
    
    Args:
        limit: Number of genres to return
        sort_by: Sort metric (popularity, count, or rating)
    
    Returns:
        GenrePopularityResponse with genre statistics
    
    Example:
        GET /report/genre-popularity?limit=10&sort_by=popularity
    """
    try:
        service = MovieAnalyticsService()
        genres = service.get_genre_popularity(limit=limit, sort_by=sort_by)
        
        return GenrePopularityResponse(
            genres=[
                GenrePopularity(
                    genre=g['genre'],
                    count=g['count'],
                    avg_popularity=round(g['avg_popularity'], 2),
                    avg_rating=round(g['avg_rating'], 2),
                )
                for g in genres
            ],
            total=len(genres)
        )
    
    except Exception as e:
        logger.error(f"Error fetching genre popularity: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail={
                "code": "QUERY_FAILED",
                "message": "Failed to fetch genre popularity"
            }
        )


@router.get(
    "/movies-by-status",
    summary="Get movie count by status",
    description="Aggregates movies by status (released, upcoming, etc.)",
)
async def get_movies_by_status():
    """
    Get movie count grouped by status.
    
    Pipeline:
    1. Group by status
    2. Count movies per status
    3. Sort by count (descending)
    
    Returns:
        List of status counts
    
    Example:
        GET /report/movies-by-status
    """
    try:
        service = MovieAnalyticsService()
        status_counts = service.get_movies_by_status()
        
        return {
            "statuses": status_counts,
            "total": len(status_counts)
        }
    
    except Exception as e:
        logger.error(f"Error fetching movies by status: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail={
                "code": "QUERY_FAILED",
                "message": "Failed to fetch movies by status"
            }
        )
