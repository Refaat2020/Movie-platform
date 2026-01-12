"""
Pydantic schemas for movie reports.

Provides typed, validated response models.
"""
from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import date


class HighestRatedMovie(BaseModel):
    """
    Schema for highest-rated movie report.
    
    Returns minimal fields for performance.
    """
    title: str = Field(..., description="Movie title")
    vote_average: float = Field(..., ge=0.0, le=10.0, description="Average rating (0-10)")
    vote_count: int = Field(..., ge=0, description="Number of votes")
    release_date: Optional[date] = Field(None, description="Release date")
    poster_path: Optional[str] = Field(None, description="Poster image URL")
    
    class Config:
        json_schema_extra = {
            "example": {
                "title": "The Shawshank Redemption",
                "vote_average": 8.7,
                "vote_count": 25000,
                "release_date": "1994-09-23",
                "poster_path": "https://image.tmdb.org/t/p/w500/poster.jpg"
            }
        }


class HighestRatedMoviesResponse(BaseModel):
    """Response for highest-rated movies endpoint."""
    movies: List[HighestRatedMovie] = Field(..., description="Top rated movies")
    total: int = Field(..., description="Total number of movies returned")
    
    class Config:
        json_schema_extra = {
            "example": {
                "movies": [
                    {
                        "title": "The Shawshank Redemption",
                        "vote_average": 8.7,
                        "vote_count": 25000,
                        "release_date": "1994-09-23",
                        "poster_path": "https://image.tmdb.org/t/p/w500/poster.jpg"
                    }
                ],
                "total": 10
            }
        }


class PopularMoviesByYear(BaseModel):
    """
    Schema for popular movies summary by year.
    
    Aggregates movies by release year with statistics.
    """
    year: int = Field(..., description="Release year")
    count: int = Field(..., ge=0, description="Number of movies released")
    avg_popularity: float = Field(..., ge=0.0, description="Average popularity score")
    avg_rating: float = Field(..., ge=0.0, le=10.0, description="Average vote rating")
    total_votes: int = Field(..., ge=0, description="Total vote count")
    
    class Config:
        json_schema_extra = {
            "example": {
                "year": 2024,
                "count": 150,
                "avg_popularity": 42.5,
                "avg_rating": 6.8,
                "total_votes": 125000
            }
        }


class PopularMoviesSummaryResponse(BaseModel):
    """Response for popular movies summary endpoint."""
    summary: List[PopularMoviesByYear] = Field(..., description="Movies grouped by year")
    total_years: int = Field(..., description="Number of years in result")
    
    class Config:
        json_schema_extra = {
            "example": {
                "summary": [
                    {
                        "year": 2024,
                        "count": 150,
                        "avg_popularity": 42.5,
                        "avg_rating": 6.8,
                        "total_votes": 125000
                    },
                    {
                        "year": 2023,
                        "count": 180,
                        "avg_popularity": 38.2,
                        "avg_rating": 6.5,
                        "total_votes": 98000
                    }
                ],
                "total_years": 2
            }
        }


class GenrePopularity(BaseModel):
    """Schema for genre popularity report."""
    genre: str = Field(..., description="Genre name")
    count: int = Field(..., ge=0, description="Number of movies")
    avg_popularity: float = Field(..., ge=0.0, description="Average popularity")
    avg_rating: float = Field(..., ge=0.0, le=10.0, description="Average rating")
    
    class Config:
        json_schema_extra = {
            "example": {
                "genre": "Action",
                "count": 250,
                "avg_popularity": 45.3,
                "avg_rating": 6.7
            }
        }


class GenrePopularityResponse(BaseModel):
    """Response for genre popularity endpoint."""
    genres: List[GenrePopularity] = Field(..., description="Genres ranked by popularity")
    total: int = Field(..., description="Total number of genres")


class ErrorResponse(BaseModel):
    """Standard error response schema."""
    error: dict = Field(..., description="Error details")
    
    class Config:
        json_schema_extra = {
            "example": {
                "error": {
                    "code": "NOT_FOUND",
                    "message": "No data found for the specified criteria"
                }
            }
        }
