"""
TMDB API Client.

Handles:
- API authentication
- Rate limiting
- Retries with exponential backoff
- Error handling
- Response parsing
"""
import logging
import time
from typing import Dict, List, Optional, Any
from decimal import Decimal
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from django.conf import settings
from django.core.cache import cache

from apps.integrations.tmdb.schemas import TMDBMovie, TMDBMovieDetail
from apps.integrations.tmdb.exceptions import (
    TMDBAPIError,
    TMDBRateLimitError,
    TMDBAuthError,
    TMDBNotFoundError,
)

logger = logging.getLogger(__name__)


class TMDBClient:
    """
    Client for TheMovieDB API v3.
    
    Features:
    - Automatic retries with exponential backoff
    - Rate limit handling with Redis-based throttling
    - Request/response logging
    - Connection pooling
    
    Usage:
        client = TMDBClient()
        movies = client.get_popular_movies(page=1)
        detail = client.get_movie_detail(movie_id=550)
    """
    
    BASE_URL = "https://api.themoviedb.org/3"
    
    # Rate limiting configuration
    RATE_LIMIT_REQUESTS = 40  # TMDB allows 40 requests per 10 seconds
    RATE_LIMIT_WINDOW = 10    # seconds
    
    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize TMDB client.
        
        Args:
            api_key: TMDB API key. If not provided, reads from settings.
        """
        self.api_key = api_key or settings.TMDB_API_KEY
        
        if not self.api_key:
            raise TMDBAuthError("TMDB API key not configured")
        
        # Configure session with retry strategy
        self.session = self._create_session()
        
        # Rate limiting cache key
        self._rate_limit_key = "tmdb:rate_limit"
    
    def _create_session(self) -> requests.Session:
        """
        Create requests session with retry strategy.
        
        Retry strategy:
        - Retry on 5xx errors and connection errors
        - Exponential backoff: 1s, 2s, 4s, 8s, 16s
        - Don't retry on 4xx (client errors) except 429 (rate limit)
        """
        session = requests.Session()
        
        # Configure retry strategy
        retry_strategy = Retry(
            total=5,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"],
        )
        
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=10,
            pool_maxsize=20,
        )
        
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        
        return session
    
    def _check_rate_limit(self):
        """
        Check and enforce rate limits using Redis.
        
        TMDB allows 40 requests per 10 seconds.
        Uses sliding window algorithm with Redis.
        
        Raises:
            TMDBRateLimitError: If rate limit would be exceeded
        """
        current_count = cache.get(self._rate_limit_key, 0)
        
        if current_count >= self.RATE_LIMIT_REQUESTS:
            logger.warning(
                "TMDB rate limit reached",
                extra={
                    'current_count': current_count,
                    'limit': self.RATE_LIMIT_REQUESTS,
                }
            )
            raise TMDBRateLimitError(
                f"Rate limit exceeded: {current_count}/{self.RATE_LIMIT_REQUESTS} "
                f"requests in {self.RATE_LIMIT_WINDOW}s window"
            )
        
        # Increment counter with TTL
        cache.set(
            self._rate_limit_key,
            current_count + 1,
            timeout=self.RATE_LIMIT_WINDOW
        )
    
    def _make_request(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        timeout: int = 10
    ) -> Dict[str, Any]:
        """
        Make authenticated request to TMDB API.
        
        Args:
            endpoint: API endpoint (e.g., "/movie/popular")
            params: Query parameters
            timeout: Request timeout in seconds
        
        Returns:
            Parsed JSON response
        
        Raises:
            TMDBAPIError: On API errors
            TMDBRateLimitError: On rate limit
            TMDBAuthError: On authentication errors
            TMDBNotFoundError: On 404 errors
        """
        # Check rate limit before making request
        self._check_rate_limit()
        
        url = f"{self.BASE_URL}{endpoint}"
        
        # Add API key to params
        request_params = params or {}
        request_params['api_key'] = self.api_key
        
        try:
            logger.debug(
                f"TMDB API request: {endpoint}",
                extra={'params': {k: v for k, v in request_params.items() if k != 'api_key'}}
            )
            
            response = self.session.get(
                url,
                params=request_params,
                timeout=timeout
            )
            
            # Log response time
            logger.debug(
                f"TMDB API response: {response.status_code}",
                extra={
                    'endpoint': endpoint,
                    'status_code': response.status_code,
                    'response_time': response.elapsed.total_seconds(),
                }
            )
            
            # Handle specific status codes
            if response.status_code == 401:
                raise TMDBAuthError("Invalid API key or unauthorized")
            
            if response.status_code == 404:
                raise TMDBNotFoundError(f"Resource not found: {endpoint}")
            
            if response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', 10))
                raise TMDBRateLimitError(
                    f"Rate limit exceeded. Retry after {retry_after}s",
                    retry_after=retry_after
                )
            
            # Raise for other HTTP errors
            response.raise_for_status()
            
            return response.json()
        
        except requests.exceptions.Timeout as e:
            logger.error(f"TMDB API timeout: {endpoint}", exc_info=True)
            raise TMDBAPIError(f"Request timeout: {endpoint}") from e
        
        except requests.exceptions.ConnectionError as e:
            logger.error(f"TMDB API connection error: {endpoint}", exc_info=True)
            raise TMDBAPIError(f"Connection error: {endpoint}") from e
        
        except requests.exceptions.HTTPError as e:
            logger.error(
                f"TMDB API HTTP error: {endpoint}",
                extra={'status_code': e.response.status_code},
                exc_info=True
            )
            raise TMDBAPIError(
                f"HTTP {e.response.status_code}: {e.response.text}"
            ) from e
        
        except ValueError as e:
            logger.error(f"TMDB API invalid JSON: {endpoint}", exc_info=True)
            raise TMDBAPIError("Invalid JSON response") from e
    
    def get_popular_movies(
        self,
        page: int = 1,
        region: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Fetch popular movies from TMDB.
        
        Args:
            page: Page number (1-1000)
            region: ISO 3166-1 country code (e.g., 'US')
        
        Returns:
            Dictionary with:
            - page: Current page number
            - results: List of movie objects
            - total_pages: Total available pages
            - total_results: Total number of movies
        
        Example:
            response = client.get_popular_movies(page=1)
            for movie in response['results']:
                print(movie['title'])
        """
        params = {'page': page}
        if region:
            params['region'] = region
        
        response = self._make_request('/movie/popular', params=params)
        
        logger.info(
            f"Fetched popular movies: page {page}",
            extra={
                'page': page,
                'results_count': len(response.get('results', [])),
                'total_pages': response.get('total_pages'),
            }
        )
        
        return response
    
    def get_movie_detail(self, movie_id: int) -> Dict[str, Any]:
        """
        Fetch detailed information for a specific movie.
        
        Args:
            movie_id: TMDB movie ID
        
        Returns:
            Detailed movie object with genres, production companies, etc.
        
        Raises:
            TMDBNotFoundError: If movie doesn't exist
        """
        response = self._make_request(
            f'/movie/{movie_id}',
            params={'append_to_response': 'credits,videos'}
        )
        
        logger.info(
            f"Fetched movie detail: {response.get('title')}",
            extra={'movie_id': movie_id}
        )
        
        return response
    
    def get_trending_movies(self, time_window: str = 'day') -> Dict[str, Any]:
        """
        Fetch trending movies.
        
        Args:
            time_window: 'day' or 'week'
        
        Returns:
            Dictionary with trending movies
        """
        if time_window not in ['day', 'week']:
            raise ValueError("time_window must be 'day' or 'week'")
        
        response = self._make_request(f'/trending/movie/{time_window}')
        
        logger.info(
            f"Fetched trending movies: {time_window}",
            extra={'results_count': len(response.get('results', []))}
        )
        
        return response
    
    def get_upcoming_movies(self, page: int = 1) -> Dict[str, Any]:
        """
        Fetch upcoming movies.
        
        Args:
            page: Page number
        
        Returns:
            Dictionary with upcoming movies
        """
        response = self._make_request(
            '/movie/upcoming',
            params={'page': page}
        )
        
        logger.info(
            f"Fetched upcoming movies: page {page}",
            extra={
                'page': page,
                'results_count': len(response.get('results', [])),
            }
        )
        
        return response
    
    def search_movies(self, query: str, page: int = 1) -> Dict[str, Any]:
        """
        Search for movies by title.
        
        Args:
            query: Search query
            page: Page number
        
        Returns:
            Dictionary with search results
        """
        response = self._make_request(
            '/search/movie',
            params={'query': query, 'page': page}
        )
        
        logger.info(
            f"Searched movies: '{query}'",
            extra={
                'query': query,
                'results_count': len(response.get('results', [])),
            }
        )
        
        return response
    
    def get_genres(self) -> List[Dict[str, Any]]:
        """
        Fetch list of official TMDB genres.
        
        Returns:
            List of genre objects with id and name
        
        Example:
            genres = client.get_genres()
            # [{'id': 28, 'name': 'Action'}, ...]
        """
        response = self._make_request('/genre/movie/list')
        
        genres = response.get('genres', [])
        
        logger.info(
            f"Fetched genres",
            extra={'count': len(genres)}
        )
        
        return genres
    
    def build_image_url(
        self,
        path: str,
        size: str = 'original'
    ) -> str:
        """
        Build full image URL from TMDB path.
        
        Args:
            path: Image path from API (e.g., '/abc123.jpg')
            size: Image size (w500, original, etc.)
        
        Returns:
            Full image URL
        
        Example:
            url = client.build_image_url('/abc123.jpg', size='w500')
            # https://image.tmdb.org/t/p/w500/abc123.jpg
        """
        if not path:
            return ''
        
        base_url = getattr(
            settings,
            'TMDB_IMAGE_BASE_URL',
            'https://image.tmdb.org/t/p'
        )
        
        return f"{base_url}/{size}{path}"
    
    def close(self):
        """Close the requests session."""
        self.session.close()
    
    def __enter__(self):
        """Context manager support."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager cleanup."""
        self.close()
