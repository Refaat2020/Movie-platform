"""
Tests for TMDB API client.

Tests cover:
- API request/response handling
- Rate limiting
- Error handling
- Retries
"""
import pytest
import responses
from requests.exceptions import Timeout, ConnectionError
from django.core.cache import cache

from apps.integrations.tmdb.client import TMDBClient
from apps.integrations.tmdb.exceptions import (
    TMDBAPIError,
    TMDBRateLimitError,
    TMDBAuthError,
    TMDBNotFoundError,
)


@pytest.fixture
def tmdb_client():
    """Fixture providing TMDB client with test API key."""
    return TMDBClient(api_key='test_api_key_12345')


@pytest.fixture(autouse=True)
def clear_rate_limit_cache():
    """Clear rate limit cache before each test."""
    cache.clear()
    yield
    cache.clear()


class TestTMDBClientInit:
    """Tests for TMDBClient initialization."""
    
    def test_client_init_with_api_key(self):
        """Test client initialization with API key."""
        client = TMDBClient(api_key='test_key')
        assert client.api_key == 'test_key'
    
    def test_client_init_without_api_key_raises_error(self, settings):
        """Test that missing API key raises error."""
        settings.TMDB_API_KEY = ''
        
        with pytest.raises(TMDBAuthError, match="API key not configured"):
            TMDBClient()
    
    def test_client_context_manager(self):
        """Test client can be used as context manager."""
        with TMDBClient(api_key='test_key') as client:
            assert client.api_key == 'test_key'


class TestGetPopularMovies:
    """Tests for get_popular_movies method."""
    
    @responses.activate
    def test_get_popular_movies_success(self, tmdb_client):
        """Test successful API request."""
        # Mock API response
        responses.add(
            responses.GET,
            'https://api.themoviedb.org/3/movie/popular',
            json={
                'page': 1,
                'results': [
                    {
                        'id': 550,
                        'title': 'Fight Club',
                        'popularity': 85.234,
                    }
                ],
                'total_pages': 10,
            },
            status=200,
        )
        
        # Act
        result = tmdb_client.get_popular_movies(page=1)
        
        # Assert
        assert result['page'] == 1
        assert len(result['results']) == 1
        assert result['results'][0]['title'] == 'Fight Club'
    
    @responses.activate
    def test_get_popular_movies_with_region(self, tmdb_client):
        """Test API request with region parameter."""
        responses.add(
            responses.GET,
            'https://api.themoviedb.org/3/movie/popular',
            json={'page': 1, 'results': []},
            status=200,
        )
        
        # Act
        tmdb_client.get_popular_movies(page=1, region='US')
        
        # Assert: Verify request was made with region parameter
        assert len(responses.calls) == 1
        assert 'region=US' in responses.calls[0].request.url


class TestRateLimiting:
    """Tests for rate limiting functionality."""
    
    def test_rate_limit_enforcement(self, tmdb_client):
        """Test that rate limit is enforced."""
        # Fill up rate limit bucket
        for _ in range(tmdb_client.RATE_LIMIT_REQUESTS):
            cache.set(
                tmdb_client._rate_limit_key,
                cache.get(tmdb_client._rate_limit_key, 0) + 1,
                timeout=tmdb_client.RATE_LIMIT_WINDOW
            )
        
        # Next request should be rate limited
        with pytest.raises(TMDBRateLimitError, match="Rate limit exceeded"):
            tmdb_client._check_rate_limit()
    
    def test_rate_limit_resets_after_window(self, tmdb_client):
        """Test that rate limit resets after time window."""
        # Set rate limit counter
        cache.set(
            tmdb_client._rate_limit_key,
            tmdb_client.RATE_LIMIT_REQUESTS,
            timeout=1  # Short TTL for testing
        )
        
        # Should be rate limited
        with pytest.raises(TMDBRateLimitError):
            tmdb_client._check_rate_limit()
        
        # Wait for cache to expire
        import time
        time.sleep(1.1)
        
        # Should work now (cache expired)
        tmdb_client._check_rate_limit()  # Should not raise


class TestErrorHandling:
    """Tests for API error handling."""
    
    @responses.activate
    def test_handles_401_unauthorized(self, tmdb_client):
        """Test handling of 401 Unauthorized."""
        responses.add(
            responses.GET,
            'https://api.themoviedb.org/3/movie/popular',
            json={'status_message': 'Invalid API key'},
            status=401,
        )
        
        with pytest.raises(TMDBAuthError, match="Invalid API key"):
            tmdb_client.get_popular_movies()
    
    @responses.activate
    def test_handles_404_not_found(self, tmdb_client):
        """Test handling of 404 Not Found."""
        responses.add(
            responses.GET,
            'https://api.themoviedb.org/3/movie/999999',
            json={'status_message': 'The resource you requested could not be found'},
            status=404,
        )
        
        with pytest.raises(TMDBNotFoundError, match="Resource not found"):
            tmdb_client._make_request('/movie/999999')
    
    @responses.activate
    def test_handles_429_rate_limit(self, tmdb_client):
        """Test handling of 429 Too Many Requests."""
        responses.add(
            responses.GET,
            'https://api.themoviedb.org/3/movie/popular',
            json={'status_message': 'Your request count is over the allowed limit'},
            status=429,
            headers={'Retry-After': '10'},
        )
        
        with pytest.raises(TMDBRateLimitError) as exc_info:
            tmdb_client.get_popular_movies()
        
        assert exc_info.value.retry_after == 10
    
    @responses.activate
    def test_handles_500_server_error(self, tmdb_client):
        """Test handling of 500 Internal Server Error."""
        responses.add(
            responses.GET,
            'https://api.themoviedb.org/3/movie/popular',
            json={'status_message': 'Internal server error'},
            status=500,
        )
        
        with pytest.raises(TMDBAPIError, match="HTTP 500"):
            tmdb_client.get_popular_movies()
    
    @responses.activate
    def test_handles_timeout(self, tmdb_client):
        """Test handling of request timeout."""
        responses.add(
            responses.GET,
            'https://api.themoviedb.org/3/movie/popular',
            body=Timeout(),
        )
        
        with pytest.raises(TMDBAPIError, match="Request timeout"):
            tmdb_client.get_popular_movies()
    
    @responses.activate
    def test_handles_connection_error(self, tmdb_client):
        """Test handling of connection error."""
        responses.add(
            responses.GET,
            'https://api.themoviedb.org/3/movie/popular',
            body=ConnectionError(),
        )
        
        with pytest.raises(TMDBAPIError, match="Connection error"):
            tmdb_client.get_popular_movies()


class TestGetMovieDetail:
    """Tests for get_movie_detail method."""
    
    @responses.activate
    def test_get_movie_detail_success(self, tmdb_client):
        """Test successful movie detail fetch."""
        responses.add(
            responses.GET,
            'https://api.themoviedb.org/3/movie/550',
            json={
                'id': 550,
                'title': 'Fight Club',
                'runtime': 139,
                'genres': [{'id': 18, 'name': 'Drama'}],
            },
            status=200,
        )
        
        # Act
        result = tmdb_client.get_movie_detail(550)
        
        # Assert
        assert result['id'] == 550
        assert result['title'] == 'Fight Club'
        assert result['runtime'] == 139


class TestGetGenres:
    """Tests for get_genres method."""
    
    @responses.activate
    def test_get_genres_success(self, tmdb_client):
        """Test successful genre fetch."""
        responses.add(
            responses.GET,
            'https://api.themoviedb.org/3/genre/movie/list',
            json={
                'genres': [
                    {'id': 28, 'name': 'Action'},
                    {'id': 12, 'name': 'Adventure'},
                ]
            },
            status=200,
        )
        
        # Act
        result = tmdb_client.get_genres()
        
        # Assert
        assert len(result) == 2
        assert result[0]['name'] == 'Action'


class TestImageURLBuilder:
    """Tests for image URL building."""
    
    def test_build_image_url_with_path(self, tmdb_client):
        """Test building image URL from path."""
        url = tmdb_client.build_image_url('/poster.jpg', size='w500')
        assert url == 'https://image.tmdb.org/t/p/w500/poster.jpg'
    
    def test_build_image_url_empty_path(self, tmdb_client):
        """Test building URL with empty path."""
        url = tmdb_client.build_image_url('', size='w500')
        assert url == ''
    
    def test_build_image_url_original_size(self, tmdb_client):
        """Test building URL with original size."""
        url = tmdb_client.build_image_url('/poster.jpg', size='original')
        assert url == 'https://image.tmdb.org/t/p/original/poster.jpg'
