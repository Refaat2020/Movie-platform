"""
Tests for FastAPI reporting endpoints.

Uses pytest with FastAPI TestClient and MongoDB in-memory or test instance.
"""
import pytest
from fastapi.testclient import TestClient
from datetime import datetime, date
from unittest.mock import Mock, patch

from reporting_api.main import app
from reporting_api.services.analytics_service import MovieAnalyticsService


@pytest.fixture
def client():
    """FastAPI test client."""
    return TestClient(app)


@pytest.fixture
def mock_movies_data():
    """Sample movie data for testing."""
    return [
        {
            "title": "The Shawshank Redemption",
            "vote_average": 8.7,
            "vote_count": 25000,
            "release_date": datetime(1994, 9, 23),
            "poster_path": "https://image.tmdb.org/poster.jpg",
            "popularity": 85.5,
            "genres": [{"name": "Drama", "slug": "drama"}],
            "status": "released"
        },
        {
            "title": "The Godfather",
            "vote_average": 8.7,
            "vote_count": 18000,
            "release_date": datetime(1972, 3, 24),
            "poster_path": "https://image.tmdb.org/poster2.jpg",
            "popularity": 90.2,
            "genres": [{"name": "Crime", "slug": "crime"}, {"name": "Drama", "slug": "drama"}],
            "status": "released"
        },
        {
            "title": "The Dark Knight",
            "vote_average": 8.5,
            "vote_count": 30000,
            "release_date": datetime(2008, 7, 18),
            "poster_path": "https://image.tmdb.org/poster3.jpg",
            "popularity": 95.8,
            "genres": [{"name": "Action", "slug": "action"}],
            "status": "released"
        }
    ]


class TestHealthEndpoints:
    """Tests for health check endpoints."""
    
    def test_health_check(self, client):
        """Test basic health check."""
        response = client.get("/health/")
        
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert "service" in data
    
    @patch('reporting_api.routers.health.get_mongodb_client')
    @patch('reporting_api.routers.health.get_movies_collection')
    def test_mongodb_health_success(self, mock_collection, mock_client, client):
        """Test MongoDB health check when connection is healthy."""
        # Mock successful ping
        mock_client.return_value.admin.command.return_value = {'ok': 1}
        
        # Mock collection count
        mock_collection.return_value.count_documents.return_value = 100
        
        response = client.get("/health/mongodb")
        
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert data["database"] == "connected"
        assert data["movies_count"] == 100


class TestHighestRatedMovies:
    """Tests for highest-rated movies endpoint."""
    
    @patch.object(MovieAnalyticsService, 'get_highest_rated_movies')
    def test_get_highest_rated_movies_success(self, mock_service, client, mock_movies_data):
        """Test successful retrieval of highest-rated movies."""
        # Mock service response
        mock_service.return_value = mock_movies_data
        
        response = client.get("/report/highest-rated-movies?limit=3&min_votes=100")
        
        assert response.status_code == 200
        data = response.json()
        
        assert "movies" in data
        assert "total" in data
        assert len(data["movies"]) == 3
        assert data["total"] == 3
        
        # Verify first movie
        first_movie = data["movies"][0]
        assert first_movie["title"] == "The Shawshank Redemption"
        assert first_movie["vote_average"] == 8.7
        assert first_movie["vote_count"] == 25000
    
    @patch.object(MovieAnalyticsService, 'get_highest_rated_movies')
    def test_get_highest_rated_movies_with_parameters(self, mock_service, client):
        """Test endpoint with custom parameters."""
        mock_service.return_value = []
        
        response = client.get("/report/highest-rated-movies?limit=5&min_votes=1000")
        
        assert response.status_code == 200
        
        # Verify service was called with correct parameters
        mock_service.assert_called_once_with(limit=5, min_votes=1000)
    
    def test_get_highest_rated_movies_invalid_limit(self, client):
        """Test validation of limit parameter."""
        # Limit too high
        response = client.get("/report/highest-rated-movies?limit=200")
        assert response.status_code == 422  # Validation error
        
        # Limit too low
        response = client.get("/report/highest-rated-movies?limit=0")
        assert response.status_code == 422
    
    @patch.object(MovieAnalyticsService, 'get_highest_rated_movies')
    def test_get_highest_rated_movies_empty_result(self, mock_service, client):
        """Test when no movies match criteria."""
        mock_service.return_value = []
        
        response = client.get("/report/highest-rated-movies?min_votes=1000000")
        
        assert response.status_code == 200
        data = response.json()
        assert data["movies"] == []
        assert data["total"] == 0


class TestPopularMoviesSummary:
    """Tests for popular movies summary endpoint."""
    
    @patch.object(MovieAnalyticsService, 'get_popular_movies_by_year')
    def test_get_popular_movies_summary_success(self, mock_service, client):
        """Test successful retrieval of popular movies summary."""
        # Mock service response
        mock_service.return_value = [
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
        ]
        
        response = client.get("/report/popular-movies-summary")
        
        assert response.status_code == 200
        data = response.json()
        
        assert "summary" in data
        assert "total_years" in data
        assert len(data["summary"]) == 2
        assert data["total_years"] == 2
        
        # Verify first year
        first_year = data["summary"][0]
        assert first_year["year"] == 2024
        assert first_year["count"] == 150
        assert first_year["avg_popularity"] == 42.5
    
    @patch.object(MovieAnalyticsService, 'get_popular_movies_by_year')
    def test_get_popular_movies_summary_with_year_range(self, mock_service, client):
        """Test endpoint with year range parameters."""
        mock_service.return_value = []
        
        response = client.get("/report/popular-movies-summary?start_year=2020&end_year=2024")
        
        assert response.status_code == 200
        
        # Verify service was called with correct parameters
        mock_service.assert_called_once_with(
            start_year=2020,
            end_year=2024,
            min_popularity=0.0
        )
    
    def test_get_popular_movies_summary_invalid_year(self, client):
        """Test validation of year parameters."""
        # Year too early
        response = client.get("/report/popular-movies-summary?start_year=1800")
        assert response.status_code == 422
        
        # Year too late
        response = client.get("/report/popular-movies-summary?end_year=2200")
        assert response.status_code == 422


class TestGenrePopularity:
    """Tests for genre popularity endpoint."""
    
    @patch.object(MovieAnalyticsService, 'get_genre_popularity')
    def test_get_genre_popularity_success(self, mock_service, client):
        """Test successful retrieval of genre popularity."""
        # Mock service response
        mock_service.return_value = [
            {
                "genre": "Action",
                "count": 250,
                "avg_popularity": 45.3,
                "avg_rating": 6.7
            },
            {
                "genre": "Drama",
                "count": 320,
                "avg_popularity": 38.5,
                "avg_rating": 7.2
            }
        ]
        
        response = client.get("/report/genre-popularity")
        
        assert response.status_code == 200
        data = response.json()
        
        assert "genres" in data
        assert "total" in data
        assert len(data["genres"]) == 2
        
        # Verify first genre
        first_genre = data["genres"][0]
        assert first_genre["genre"] == "Action"
        assert first_genre["count"] == 250
    
    @patch.object(MovieAnalyticsService, 'get_genre_popularity')
    def test_get_genre_popularity_sort_by_count(self, mock_service, client):
        """Test sorting by count."""
        mock_service.return_value = []
        
        response = client.get("/report/genre-popularity?sort_by=count")
        
        assert response.status_code == 200
        mock_service.assert_called_once_with(limit=20, sort_by='count')
    
    def test_get_genre_popularity_invalid_sort(self, client):
        """Test validation of sort_by parameter."""
        response = client.get("/report/genre-popularity?sort_by=invalid")
        assert response.status_code == 422


class TestMovieAnalyticsService:
    """Tests for MovieAnalyticsService aggregation logic."""
    
    @patch('reporting_api.services.movie_analytics.get_movies_collection')
    def test_get_highest_rated_movies_pipeline(self, mock_collection):
        """Test highest-rated movies aggregation pipeline."""
        # Mock collection
        mock_aggregate = Mock(return_value=[
            {
                "title": "Test Movie",
                "vote_average": 9.0,
                "vote_count": 5000,
                "release_date": datetime(2020, 1, 1),
                "poster_path": "https://test.jpg"
            }
        ])
        mock_collection.return_value.aggregate = mock_aggregate
        
        # Execute
        service = MovieAnalyticsService()
        results = service.get_highest_rated_movies(limit=10, min_votes=100)
        
        # Verify
        assert len(results) == 1
        assert results[0]["title"] == "Test Movie"
        
        # Verify pipeline was called
        mock_aggregate.assert_called_once()
        pipeline = mock_aggregate.call_args[0][0]
        
        # Verify pipeline stages
        assert pipeline[0]['$match']['vote_count']['$gte'] == 100
        assert pipeline[1]['$sort']['vote_average'] == -1
        assert pipeline[2]['$limit'] == 10
    
    @patch('reporting_api.services.movie_analytics.get_movies_collection')
    def test_get_popular_movies_by_year_pipeline(self, mock_collection):
        """Test popular movies by year aggregation pipeline."""
        # Mock collection
        mock_aggregate = Mock(return_value=[
            {
                "year": 2024,
                "count": 100,
                "avg_popularity": 50.0,
                "avg_rating": 7.0,
                "total_votes": 10000
            }
        ])
        mock_collection.return_value.aggregate = mock_aggregate
        
        # Execute
        service = MovieAnalyticsService()
        results = service.get_popular_movies_by_year(
            start_year=2020,
            end_year=2024,
            min_popularity=10.0
        )
        
        # Verify
        assert len(results) == 1
        assert results[0]["year"] == 2024
        
        # Verify pipeline was called
        mock_aggregate.assert_called_once()


class TestResponseSchemas:
    """Tests for response schema validation."""
    
    def test_highest_rated_movie_schema(self):
        """Test HighestRatedMovie schema validation."""
        from reporting_api.schemas.movies import HighestRatedMovie
        
        # Valid data
        movie = HighestRatedMovie(
            title="Test Movie",
            vote_average=8.5,
            vote_count=1000,
            release_date=date(2020, 1, 1),
            poster_path="https://test.jpg"
        )
        
        assert movie.title == "Test Movie"
        assert movie.vote_average == 8.5
    
    def test_popular_movies_by_year_schema(self):
        """Test PopularMoviesByYear schema validation."""
        from reporting_api.schemas.movies import PopularMoviesByYear
        
        # Valid data
        summary = PopularMoviesByYear(
            year=2024,
            count=100,
            avg_popularity=50.0,
            avg_rating=7.5,
            total_votes=10000
        )
        
        assert summary.year == 2024
        assert summary.count == 100


class TestErrorHandling:
    """Tests for error handling."""
    
    @patch.object(MovieAnalyticsService, 'get_highest_rated_movies')
    def test_service_exception_handling(self, mock_service, client):
        """Test that service exceptions are handled gracefully."""
        # Mock service to raise exception
        mock_service.side_effect = Exception("Database error")
        
        response = client.get("/report/highest-rated-movies")
        
        assert response.status_code == 500
        data = response.json()
        assert "detail" in data
        assert data["detail"]["code"] == "QUERY_FAILED"
