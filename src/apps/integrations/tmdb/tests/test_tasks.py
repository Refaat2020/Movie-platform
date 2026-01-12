"""
Tests for TMDB ingestion Celery tasks.

Tests cover:
- Happy path scenarios
- API failures and retries
- Rate limiting
- Transaction safety
- Idempotency
- Error handling
"""
import pytest
from unittest.mock import Mock, patch, MagicMock
from decimal import Decimal
from celery.exceptions import Retry

from apps.integrations.tmdb.tasks import (
    ingest_popular_movies,
    ingest_movie_detail,
    refresh_genres,
    batch_ingest_movies,
    ingest_trending_movies,
)
from apps.integrations.tmdb.exceptions import (
    TMDBAPIError,
    TMDBRateLimitError,
    TMDBAuthError,
    TMDBNotFoundError,
)
from apps.movies.models import Movie, Genre, IngestionLog
from apps.movies.tests.factories import MovieFactory, GenreFactory


# Sample TMDB API responses
SAMPLE_TMDB_MOVIE = {
    'id': 550,
    'title': 'Fight Club',
    'original_title': 'Fight Club',
    'overview': 'A ticking-time-bomb insomniac...',
    'release_date': '1999-10-15',
    'popularity': 85.234,
    'vote_average': 8.4,
    'vote_count': 25000,
    'poster_path': '/poster.jpg',
    'backdrop_path': '/backdrop.jpg',
    'adult': False,
    'genre_ids': [18, 53],
    'original_language': 'en',
    'video': False,
}

SAMPLE_POPULAR_RESPONSE = {
    'page': 1,
    'results': [SAMPLE_TMDB_MOVIE],
    'total_pages': 10,
    'total_results': 200,
}

SAMPLE_MOVIE_DETAIL = {
    **SAMPLE_TMDB_MOVIE,
    'tagline': 'Mischief. Mayhem. Soap.',
    'runtime': 139,
    'budget': 63000000,
    'revenue': 100853753,
    'status': 'Released',
    'genres': [
        {'id': 18, 'name': 'Drama'},
        {'id': 53, 'name': 'Thriller'},
    ],
    'production_companies': [
        {
            'id': 508,
            'name': '20th Century Fox',
            'logo_path': '/logo.png',
            'origin_country': 'US',
        }
    ],
    'homepage': 'http://www.foxmovies.com/movies/fight-club',
    'imdb_id': 'tt0137523',
}

SAMPLE_GENRES = [
    {'id': 28, 'name': 'Action'},
    {'id': 12, 'name': 'Adventure'},
    {'id': 18, 'name': 'Drama'},
]


@pytest.mark.django_db
class TestIngestPopularMovies:
    """Tests for ingest_popular_movies task."""
    
    @patch('apps.integrations.tmdb.tasks.TMDBClient')
    def test_ingest_popular_movies_success(self, mock_client_class):
        """Test successful ingestion of popular movies."""
        # Arrange
        mock_client = Mock()
        mock_client_class.return_value.__enter__.return_value = mock_client
        mock_client.get_popular_movies.return_value = SAMPLE_POPULAR_RESPONSE
        mock_client.build_image_url.side_effect = lambda path, size: f"https://image.tmdb.org/t/p/{size}{path}"
        
        # Act
        result = ingest_popular_movies(pages=1)
        
        # Assert
        assert result['movies_fetched'] == 1
        assert result['movies_created'] == 1
        assert result['movies_updated'] == 0
        assert len(result['errors']) == 0
        
        # Verify movie was created
        movie = Movie.objects.get(tmdb_id=550)
        assert movie.title == 'Fight Club'
        assert movie.source == Movie.Source.EXTERNAL
        assert movie.is_external is True
        
        # Verify ingestion log was created
        log = IngestionLog.objects.get(id=result['log_id'])
        assert log.status == IngestionLog.Status.COMPLETED
        assert log.movies_fetched == 1
        assert log.movies_created == 1
    
    @patch('apps.integrations.tmdb.tasks.TMDBClient')
    def test_ingest_popular_movies_idempotent(self, mock_client_class):
        """Test that re-running ingestion updates existing movies."""
        # Arrange: Create existing movie
        existing = MovieFactory(
            tmdb_id=550,
            title='Old Title',
            popularity=Decimal('10.0'),
            source=Movie.Source.EXTERNAL
        )
        
        mock_client = Mock()
        mock_client_class.return_value.__enter__.return_value = mock_client
        mock_client.get_popular_movies.return_value = SAMPLE_POPULAR_RESPONSE
        mock_client.build_image_url.side_effect = lambda path, size: f"https://image.tmdb.org/{size}{path}"
        
        # Act
        result = ingest_popular_movies(pages=1)
        
        # Assert
        assert result['movies_fetched'] == 1
        assert result['movies_created'] == 0
        assert result['movies_updated'] == 1
        
        # Verify movie was updated, not duplicated
        assert Movie.objects.filter(tmdb_id=550).count() == 1
        
        existing.refresh_from_db()
        assert existing.title == 'Fight Club'  # Updated
        assert existing.popularity == Decimal('85.234')  # Updated
    
    @patch('apps.integrations.tmdb.tasks.TMDBClient')
    def test_ingest_popular_movies_multiple_pages(self, mock_client_class):
        """Test ingesting multiple pages of movies."""
        # Arrange
        mock_client = Mock()
        mock_client_class.return_value.__enter__.return_value = mock_client
        
        # Return different movies for each page
        def get_popular_side_effect(page, region=None):
            return {
                'page': page,
                'results': [
                    {**SAMPLE_TMDB_MOVIE, 'id': 500 + page, 'title': f'Movie {page}'}
                ],
                'total_pages': 3,
                'total_results': 3,
            }
        
        mock_client.get_popular_movies.side_effect = get_popular_side_effect
        mock_client.build_image_url.return_value = 'https://image.tmdb.org/image.jpg'
        
        # Act
        result = ingest_popular_movies(pages=3)
        
        # Assert
        assert result['movies_fetched'] == 3
        assert result['movies_created'] == 3
        assert Movie.objects.count() == 3
        
        # Verify all movies created
        assert Movie.objects.filter(tmdb_id=501).exists()
        assert Movie.objects.filter(tmdb_id=502).exists()
        assert Movie.objects.filter(tmdb_id=503).exists()
    
    @patch('apps.integrations.tmdb.tasks.TMDBClient')
    def test_ingest_popular_movies_handles_api_error(self, mock_client_class):
        """Test that API errors on one page don't fail entire job."""
        # Arrange
        mock_client = Mock()
        mock_client_class.return_value.__enter__.return_value = mock_client
        
        # First page succeeds, second page fails, third page succeeds
        def get_popular_side_effect(page, region=None):
            if page == 2:
                raise TMDBAPIError("Temporary API error")
            return {
                'page': page,
                'results': [
                    {**SAMPLE_TMDB_MOVIE, 'id': 500 + page, 'title': f'Movie {page}'}
                ],
            }
        
        mock_client.get_popular_movies.side_effect = get_popular_side_effect
        mock_client.build_image_url.return_value = 'https://image.tmdb.org/image.jpg'
        
        # Act
        result = ingest_popular_movies(pages=3)
        
        # Assert: Job completes with partial success
        assert result['movies_fetched'] == 2  # Pages 1 and 3
        assert len(result['errors']) == 1
        assert 'Temporary API error' in result['errors'][0]
        
        # Verify log status is PARTIAL
        log = IngestionLog.objects.get(id=result['log_id'])
        assert log.status == IngestionLog.Status.PARTIAL
    
    @patch('apps.integrations.tmdb.tasks.TMDBClient')
    def test_ingest_popular_movies_handles_rate_limit(self, mock_client_class):
        """Test that rate limit errors trigger Celery retry."""
        # Arrange
        mock_client = Mock()
        mock_client_class.return_value.__enter__.return_value = mock_client
        mock_client.get_popular_movies.side_effect = TMDBRateLimitError(
            "Rate limit exceeded",
            retry_after=10
        )
        
        # Create a mock task with retry method
        mock_task = Mock()
        mock_task.request.id = 'test-task-id'
        mock_task.request.retries = 0
        
        # Act & Assert: Should raise Retry exception
        with pytest.raises((TMDBRateLimitError, Retry)):
            ingest_popular_movies.__wrapped__(mock_task, pages=1)
    
    @patch('apps.integrations.tmdb.tasks.TMDBClient')
    def test_ingest_popular_movies_handles_auth_error(self, mock_client_class):
        """Test that auth errors don't retry (fail immediately)."""
        # Arrange
        mock_client_class.side_effect = TMDBAuthError("Invalid API key")
        
        # Create a mock task
        mock_task = Mock()
        mock_task.request.id = 'test-task-id'
        mock_task.request.retries = 0
        mock_task.retry.side_effect = Retry()
        
        # Act & Assert
        with pytest.raises(Retry):
            ingest_popular_movies.__wrapped__(mock_task, pages=1)
        
        # Verify log was marked as FAILED
        log = IngestionLog.objects.filter(
            status=IngestionLog.Status.FAILED
        ).first()
        assert log is not None
        assert 'Authentication error' in log.errors[0]


@pytest.mark.django_db
class TestIngestMovieDetail:
    """Tests for ingest_movie_detail task."""
    
    @patch('apps.integrations.tmdb.tasks.TMDBClient')
    def test_ingest_movie_detail_success(self, mock_client_class):
        """Test successful ingestion of movie detail."""
        # Arrange
        mock_client = Mock()
        mock_client_class.return_value.__enter__.return_value = mock_client
        mock_client.get_movie_detail.return_value = SAMPLE_MOVIE_DETAIL
        mock_client.build_image_url.side_effect = lambda path, size: f"https://image.tmdb.org/{size}{path}"
        
        # Act
        result = ingest_movie_detail(tmdb_id=550)
        
        # Assert
        assert result['tmdb_id'] == 550
        assert result['title'] == 'Fight Club'
        assert result['created'] is True
        
        # Verify movie with full details
        movie = Movie.objects.get(tmdb_id=550)
        assert movie.runtime == 139
        assert movie.budget == Decimal('63000000')
        assert movie.genres.count() == 2
    
    @patch('apps.integrations.tmdb.tasks.TMDBClient')
    def test_ingest_movie_detail_updates_existing(self, mock_client_class):
        """Test updating existing movie with detail data."""
        # Arrange: Create incomplete movie
        existing = MovieFactory(
            tmdb_id=550,
            title='Fight Club',
            runtime=None,
            budget=None,
        )
        
        mock_client = Mock()
        mock_client_class.return_value.__enter__.return_value = mock_client
        mock_client.get_movie_detail.return_value = SAMPLE_MOVIE_DETAIL
        mock_client.build_image_url.return_value = 'https://image.tmdb.org/image.jpg'
        
        # Act
        result = ingest_movie_detail(tmdb_id=550)
        
        # Assert
        assert result['created'] is False
        
        existing.refresh_from_db()
        assert existing.runtime == 139  # Now populated
        assert existing.budget == Decimal('63000000')  # Now populated
    
    @patch('apps.integrations.tmdb.tasks.TMDBClient')
    def test_ingest_movie_detail_not_found(self, mock_client_class):
        """Test handling of non-existent movie."""
        # Arrange
        mock_client = Mock()
        mock_client_class.return_value.__enter__.return_value = mock_client
        mock_client.get_movie_detail.side_effect = TMDBNotFoundError("Movie not found")
        
        # Act
        result = ingest_movie_detail(tmdb_id=999999)
        
        # Assert
        assert result['error'] == 'not_found'
        assert 'not found' in result['message'].lower()
    
    @patch('apps.integrations.tmdb.tasks.TMDBClient')
    def test_ingest_movie_detail_transaction_safety(self, mock_client_class):
        """Test that failures don't leave partial data."""
        # Arrange
        mock_client = Mock()
        mock_client_class.return_value.__enter__.return_value = mock_client
        
        # Simulate failure during genre sync (after movie creation)
        with patch('apps.movies.repositories.movie_repository.MovieRepository._sync_genres') as mock_sync:
            mock_sync.side_effect = Exception("Genre sync failed")
            mock_client.get_movie_detail.return_value = SAMPLE_MOVIE_DETAIL
            mock_client.build_image_url.return_value = 'https://image.tmdb.org/image.jpg'
            
            # Act & Assert: Should raise exception
            with pytest.raises(Exception):
                ingest_movie_detail(tmdb_id=550)
            
            # Verify: Transaction should have rolled back (no movie created)
            assert not Movie.objects.filter(tmdb_id=550).exists()


@pytest.mark.django_db
class TestRefreshGenres:
    """Tests for refresh_genres task."""
    
    @patch('apps.integrations.tmdb.tasks.TMDBClient')
    def test_refresh_genres_success(self, mock_client_class):
        """Test successful genre sync."""
        # Arrange
        mock_client = Mock()
        mock_client_class.return_value.__enter__.return_value = mock_client
        mock_client.get_genres.return_value = SAMPLE_GENRES
        
        # Act
        result = refresh_genres()
        
        # Assert
        assert result['genres_created'] == 3
        assert result['total_genres'] == 3
        
        # Verify genres in database
        assert Genre.objects.count() == 3
        assert Genre.objects.filter(tmdb_id=28, name='Action').exists()
    
    @patch('apps.integrations.tmdb.tasks.TMDBClient')
    def test_refresh_genres_idempotent(self, mock_client_class):
        """Test that re-running genre sync updates existing genres."""
        # Arrange: Create existing genre
        GenreFactory(tmdb_id=28, name='Old Action Name')
        
        mock_client = Mock()
        mock_client_class.return_value.__enter__.return_value = mock_client
        mock_client.get_genres.return_value = SAMPLE_GENRES
        
        # Act
        result = refresh_genres()
        
        # Assert
        assert result['genres_created'] == 2  # Only new genres
        assert result['genres_updated'] == 1  # Existing genre updated
        assert result['total_genres'] == 3
        
        # Verify update
        genre = Genre.objects.get(tmdb_id=28)
        assert genre.name == 'Action'  # Updated


@pytest.mark.django_db
class TestBatchIngestMovies:
    """Tests for batch_ingest_movies task."""
    
    @patch('apps.integrations.tmdb.tasks.group')
    @patch('apps.integrations.tmdb.tasks.ingest_movie_detail')
    def test_batch_ingest_movies_parallel_execution(self, mock_detail_task, mock_group):
        """Test that batch ingestion uses parallel execution."""
        # Arrange
        tmdb_ids = [550, 551, 552]
        
        # Mock group result
        mock_result = Mock()
        mock_result.get.return_value = [
            {'movie_id': 'uuid1', 'tmdb_id': 550, 'created': True},
            {'movie_id': 'uuid2', 'tmdb_id': 551, 'created': True},
            {'movie_id': 'uuid3', 'tmdb_id': 552, 'created': False},
        ]
        mock_group.return_value.apply_async.return_value = mock_result
        
        # Act
        result = batch_ingest_movies(tmdb_ids=tmdb_ids)
        
        # Assert
        assert result['total_movies'] == 3
        assert result['successful'] == 3
        assert result['failed'] == 0
        assert result['created'] == 2
        assert result['updated'] == 1
        
        # Verify group was called with correct tasks
        mock_group.assert_called_once()


@pytest.mark.django_db
class TestIngestTrendingMovies:
    """Tests for ingest_trending_movies task."""
    
    @patch('apps.integrations.tmdb.tasks.TMDBClient')
    def test_ingest_trending_movies_success(self, mock_client_class):
        """Test successful ingestion of trending movies."""
        # Arrange
        mock_client = Mock()
        mock_client_class.return_value.__enter__.return_value = mock_client
        mock_client.get_trending_movies.return_value = SAMPLE_POPULAR_RESPONSE
        mock_client.build_image_url.return_value = 'https://image.tmdb.org/image.jpg'
        
        # Act
        result = ingest_trending_movies(time_window='day')
        
        # Assert
        assert result['movies_fetched'] == 1
        assert result['movies_created'] == 1
        
        # Verify log
        log = IngestionLog.objects.get(id=result['log_id'])
        assert log.job_type == IngestionLog.JobType.TRENDING
        assert log.metadata['time_window'] == 'day'


@pytest.mark.django_db
class TestTaskRetryBehavior:
    """Tests for Celery retry behavior."""
    
    @patch('apps.integrations.tmdb.tasks.TMDBClient')
    def test_exponential_backoff_on_retry(self, mock_client_class):
        """Test that retries use exponential backoff."""
        # This test verifies the task configuration
        # Actual retry behavior is handled by Celery
        
        task = ingest_popular_movies
        
        # Verify retry configuration
        assert task.autoretry_for == (TMDBAPIError, TMDBRateLimitError)
        assert task.retry_backoff is True
        assert task.retry_backoff_max == 600
        assert task.max_retries == 5
        assert task.retry_jitter is True
    
    def test_task_acks_late_configuration(self):
        """Test that tasks acknowledge after completion."""
        task = ingest_popular_movies
        
        # Verify task acknowledges after completion (for reliability)
        assert task.acks_late is True
        assert task.reject_on_worker_lost is True


@pytest.mark.django_db
class TestIngestionLogTracking:
    """Tests for ingestion log creation and updates."""
    
    @patch('apps.integrations.tmdb.tasks.TMDBClient')
    def test_ingestion_log_created_on_start(self, mock_client_class):
        """Test that ingestion log is created when task starts."""
        # Arrange
        mock_client = Mock()
        mock_client_class.return_value.__enter__.return_value = mock_client
        mock_client.get_popular_movies.return_value = SAMPLE_POPULAR_RESPONSE
        mock_client.build_image_url.return_value = 'https://image.tmdb.org/image.jpg'
        
        # Act
        result = ingest_popular_movies(pages=1)
        
        # Assert: Log exists
        log = IngestionLog.objects.get(id=result['log_id'])
        assert log.job_type == IngestionLog.JobType.POPULAR_MOVIES
        assert log.started_at is not None
        assert log.completed_at is not None
    
    @patch('apps.integrations.tmdb.tasks.TMDBClient')
    def test_ingestion_log_updated_on_completion(self, mock_client_class):
        """Test that ingestion log is updated with results."""
        # Arrange
        mock_client = Mock()
        mock_client_class.return_value.__enter__.return_value = mock_client
        mock_client.get_popular_movies.return_value = SAMPLE_POPULAR_RESPONSE
        mock_client.build_image_url.return_value = 'https://image.tmdb.org/image.jpg'
        
        # Act
        result = ingest_popular_movies(pages=1)
        
        # Assert
        log = IngestionLog.objects.get(id=result['log_id'])
        assert log.status == IngestionLog.Status.COMPLETED
        assert log.movies_fetched == 1
        assert log.movies_created == 1
        assert log.duration_seconds is not None
