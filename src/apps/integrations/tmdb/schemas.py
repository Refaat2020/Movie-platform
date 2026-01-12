"""
Data schemas for TMDB API responses.

Provides type hints and data classes for API responses.
Not enforced at runtime, but useful for IDE autocomplete and documentation.
"""
from typing import List, Optional, Dict, Any
from dataclasses import dataclass
from datetime import date


@dataclass
class TMDBGenre:
    """TMDB genre object."""
    id: int
    name: str


@dataclass
class TMDBProductionCompany:
    """TMDB production company object."""
    id: int
    name: str
    logo_path: Optional[str] = None
    origin_country: Optional[str] = None


@dataclass
class TMDBMovie:
    """
    TMDB movie object from list endpoints.
    
    Lightweight representation without full details.
    """
    id: int
    title: str
    original_title: str
    overview: str
    release_date: Optional[str]
    popularity: float
    vote_average: float
    vote_count: int
    poster_path: Optional[str]
    backdrop_path: Optional[str]
    adult: bool
    genre_ids: List[int]
    original_language: str
    video: bool


@dataclass
class TMDBMovieDetail:
    """
    TMDB movie object from detail endpoint.
    
    Complete representation with all fields.
    """
    id: int
    title: str
    original_title: str
    overview: str
    tagline: str
    release_date: Optional[str]
    runtime: Optional[int]
    budget: int
    revenue: int
    popularity: float
    vote_average: float
    vote_count: int
    poster_path: Optional[str]
    backdrop_path: Optional[str]
    status: str
    adult: bool
    genres: List[Dict[str, Any]]
    production_companies: List[Dict[str, Any]]
    homepage: Optional[str]
    imdb_id: Optional[str]
    original_language: str
    spoken_languages: List[Dict[str, Any]]
