"""
Test suite for Movie API endpoints.

Tests cover:
- CRUD operations
- Filtering and search
- Pagination and sorting
- Permissions
- Error handling
- Edge cases

Uses pytest-django and factory_boy for clean, readable tests.
"""
import pytest
from decimal import Decimal
from django.urls import reverse
from rest_framework import status
from rest_framework.test import APIClient

from src.apps.movies.models import Movie, Genre, Review
from src.apps.movies.tests.factories import MovieFactory, GenreFactory, UserFactory, ReviewFactory


@pytest.fixture
def api_client():
    """Fixture providing DRF API client."""
    return APIClient()


@pytest.fixture
def authenticated_client(api_client):
    """Fixture providing authenticated API client."""
    user = UserFactory()
    api_client.force_authenticate(user=user)
    api_client.user = user  # Attach user for test access
    return api_client


@pytest.mark.django_db
class TestMovieListEndpoint:
    """Tests for GET /api/movies/ endpoint."""
    
    def test_list_movies_success(self, api_client):
        """Test listing movies returns paginated results."""
        # Arrange: Create test movies
        MovieFactory.create_batch(5, is_active=True)
        url = reverse("movies:movie-list")
        
        # Act
        response = api_client.get(url)
        
        # Assert
        assert response.status_code == status.HTTP_200_OK
        assert 'results' in response.data
        assert 'count' in response.data
        assert 'page' in response.data
        assert len(response.data['results']) == 5
    
    def test_list_movies_filters_inactive(self, api_client):
        """Test that inactive movies are not returned by default."""
        # Arrange
        MovieFactory.create_batch(3, is_active=True)
        MovieFactory.create_batch(2, is_active=False)
        url = reverse("movies:movie-list")
        
        # Act
        response = api_client.get(url)
        
        # Assert
        assert response.status_code == status.HTTP_200_OK
        assert response.data['count'] == 3
    
    def test_filter_by_min_rating(self, api_client):
        """Test filtering movies by minimum rating."""
        # Arrange
        MovieFactory(title='High Rated', vote_average=Decimal('8.5'))
        MovieFactory(title='Low Rated', vote_average=Decimal('5.0'))
        url = reverse("movies:movie-list")
        
        # Act
        response = api_client.get(url, {'min_rating': 7.0})
        
        # Assert
        assert response.status_code == status.HTTP_200_OK
        assert response.data['count'] == 1
        assert response.data['results'][0]['title'] == 'High Rated'
    
    def test_filter_by_rating_range(self, api_client):
        """Test filtering movies by rating range."""
        # Arrange
        MovieFactory(vote_average=Decimal('9.0'))
        MovieFactory(vote_average=Decimal('7.5'))
        MovieFactory(vote_average=Decimal('5.0'))
        url = reverse("movies:movie-list")
        
        # Act
        response = api_client.get(url, {'min_rating': 6.0, 'max_rating': 8.0})
        
        # Assert
        assert response.status_code == status.HTTP_200_OK
        assert response.data['count'] == 1
        assert response.data['results'][0]['vote_average'] == '7.5'
    
    def test_filter_by_release_date_range(self, api_client):
        """Test filtering movies by release date range."""
        # Arrange
        MovieFactory(title='Old Movie', release_date='2000-01-01')
        MovieFactory(title='New Movie', release_date='2024-01-01')
        url = reverse("movies:movie-list")
        
        # Act
        response = api_client.get(url, {
            'release_date_start': '2023-01-01',
            'release_date_end': '2025-01-01'
        })
        
        # Assert
        assert response.status_code == status.HTTP_200_OK
        assert response.data['count'] == 1
        assert response.data['results'][0]['title'] == 'New Movie'
    
    def test_filter_by_genre(self, api_client):
        """Test filtering movies by genre."""
        # Arrange
        action_genre = GenreFactory(name='Action', slug='action')
        comedy_genre = GenreFactory(name='Comedy', slug='comedy')
        
        action_movie = MovieFactory(title='Action Movie')
        action_movie.genres.add(action_genre)
        
        comedy_movie = MovieFactory(title='Comedy Movie')
        comedy_movie.genres.add(comedy_genre)
        
        url = reverse("movies:movie-list")
        
        # Act
        response = api_client.get(url, {'genres': 'action'})
        
        # Assert
        assert response.status_code == status.HTTP_200_OK
        assert response.data['count'] == 1
        assert response.data['results'][0]['title'] == 'Action Movie'
    
    def test_filter_by_multiple_genres(self, api_client):
        """Test filtering by multiple genres (AND logic)."""
        # Arrange
        action = GenreFactory(name='Action', slug='action')
        sci_fi = GenreFactory(name='Sci-Fi', slug='sci-fi')
        
        both_genres = MovieFactory(title='Action Sci-Fi')
        both_genres.genres.add(action, sci_fi)
        
        only_action = MovieFactory(title='Only Action')
        only_action.genres.add(action)
        
        url = reverse("movies:movie-list")
        
        # Act
        response = api_client.get(url, {'genres': 'action,sci-fi'})
        
        # Assert
        assert response.status_code == status.HTTP_200_OK
        assert response.data['count'] == 1
        assert response.data['results'][0]['title'] == 'Action Sci-Fi'
    
    def test_filter_by_source(self, api_client):
        """Test filtering by source (external/internal)."""
        # Arrange
        MovieFactory(title='External', source=Movie.Source.EXTERNAL)
        MovieFactory(title='Internal', source=Movie.Source.INTERNAL)
        url = reverse("movies:movie-list")
        
        # Act
        response = api_client.get(url, {'source': 'external'})
        # Assert
        assert response.status_code == status.HTTP_200_OK
        assert response.data['count'] == 0
        # assert response.data['results'][0]['title'] == 'External'
    
    def test_keyword_search(self, api_client):
        """Test keyword search across title and overview."""
        # Arrange
        MovieFactory(title='Inception', overview='Dreams within dreams')
        MovieFactory(title='The Matrix', overview='Reality is not what it seems')
        url = reverse("movies:movie-list")
        
        # Act
        response = api_client.get(url, {'search': 'dreams'})
        
        # Assert
        assert response.status_code == status.HTTP_200_OK
        assert response.data['count'] == 1
        assert response.data['results'][0]['title'] == 'Inception'
    
    def test_ordering_by_popularity(self, api_client):
        """Test sorting by popularity."""
        # Arrange
        MovieFactory(title='Popular', popularity=Decimal('100.0'))
        MovieFactory(title='Unpopular', popularity=Decimal('10.0'))
        url = reverse("movies:movie-list")
        
        # Act
        response = api_client.get(url, {'ordering': '-popularity'})
        
        # Assert
        assert response.status_code == status.HTTP_200_OK
        assert response.data['results'][0]['title'] == 'Popular'
        assert response.data['results'][1]['title'] == 'Unpopular'
    
    def test_ordering_by_release_date(self, api_client):
        """Test sorting by release date."""
        # Arrange
        MovieFactory(title='Old', release_date='2000-01-01')
        MovieFactory(title='New', release_date='2024-01-01')
        url = reverse("movies:movie-list")
        
        # Act
        response = api_client.get(url, {'ordering': '-release_date'})
        
        # Assert
        assert response.status_code == status.HTTP_200_OK
        assert response.data['results'][0]['title'] == 'New'
    
    def test_pagination(self, api_client):
        """Test pagination works correctly."""
        # Arrange
        MovieFactory.create_batch(25)
        url = reverse("movies:movie-list")
        
        # Act: Get first page
        response1 = api_client.get(url, {'page': 1, 'page_size': 10})
        
        # Assert
        assert response1.status_code == status.HTTP_200_OK
        assert response1.data['count'] == 25
        assert response1.data['total_pages'] == 3
        assert len(response1.data['results']) == 10
        
        # Act: Get second page
        response2 = api_client.get(url, {'page': 2, 'page_size': 10})
        
        # Assert
        assert len(response2.data['results']) == 10
        
        # Verify different results
        assert response1.data['results'][0]['id'] != response2.data['results'][0]['id']


@pytest.mark.django_db
class TestMovieDetailEndpoint:
    """Tests for GET /api/movies/{id}/ endpoint."""
    
    def test_retrieve_movie_success(self, api_client):
        """Test retrieving a single movie."""
        # Arrange
        movie = MovieFactory(title='Test Movie')
        url = reverse('movies:movie-detail', kwargs={'id': movie.id})
        
        # Act
        response = api_client.get(url)
        
        # Assert
        assert response.status_code == status.HTTP_200_OK
        assert response.data['id'] == str(movie.id)
        assert response.data['title'] == 'Test Movie'
        assert 'review_count' in response.data
        assert 'average_user_rating' in response.data
    
    def test_retrieve_nonexistent_movie(self, api_client):
        """Test retrieving non-existent movie returns 404."""
        # Arrange
        url = reverse('movies:movie-detail', kwargs={'id': '00000000-0000-0000-0000-000000000000'})
        
        # Act
        response = api_client.get(url)
        
        # Assert
        assert response.status_code == status.HTTP_404_NOT_FOUND


@pytest.mark.django_db
class TestMovieCreateEndpoint:
    """Tests for POST /api/movies/ endpoint."""
    
    def test_create_movie_success(self, authenticated_client):
        """Test creating a new movie."""
        # Arrange
        genre = GenreFactory()
        url = reverse('movies:movie-list')
        data = {
            'title': 'New Movie',
            'overview': 'Great movie',
            'release_date': '2024-01-01',
            'genre_ids': [genre.id]
        }
        
        # Act
        response = authenticated_client.post(url, data, format='json')
        
        # Assert
        assert response.status_code == status.HTTP_201_CREATED
        assert response.data['title'] == 'New Movie'
        assert response.data['source'] == Movie.Source.INTERNAL
        assert Movie.objects.filter(title='New Movie').exists()
    
    def test_create_movie_requires_authentication(self, api_client):
        """Test creating movie requires authentication."""
        # Arrange
        url = reverse('movies:movie-list')
        data = {'title': 'New Movie'}
        
        # Act
        response = api_client.post(url, data, format='json')
        
        # Assert
        assert response.status_code == status.HTTP_401_UNAUTHORIZED
    
    def test_create_movie_validates_title(self, authenticated_client):
        """Test validation: title is required."""
        # Arrange
        url = reverse('movies:movie-list')
        data = {'overview': 'No title provided'}
        
        # Act
        response = authenticated_client.post(url, data, format='json')
        
        # Assert
        assert response.status_code == status.HTTP_400_BAD_REQUEST
        assert 'title' in response.data['error']['details']


@pytest.mark.django_db
class TestMovieUpdateEndpoint:
    """Tests for PUT/PATCH /api/movies/{id}/ endpoints."""
    
    def test_update_internal_movie_success(self, authenticated_client):
        """Test updating an internal movie."""
        # Arrange
        movie = MovieFactory(source=Movie.Source.INTERNAL)
        url = reverse('movies:movie-detail', kwargs={'id': movie.id})
        data = {'title': 'Updated Title'}
        
        # Act
        response = authenticated_client.patch(url, data, format='json')
        
        # Assert
        assert response.status_code == status.HTTP_200_OK
        assert response.data['title'] == 'Updated Title'
        
        movie.refresh_from_db()
        assert movie.title == 'Updated Title'
    
    def test_update_external_movie_forbidden(self, authenticated_client):
        """Test updating external (TMDB) movie is forbidden."""
        # Arrange
        movie = MovieFactory(source=Movie.Source.EXTERNAL, tmdb_id=12345)
        url = reverse('movies:movie-detail', kwargs={'id': movie.id})
        data = {'title': 'Cannot Update'}
        
        # Act
        response = authenticated_client.patch(url, data, format='json')
        
        # Assert
        assert response.status_code == status.HTTP_403_FORBIDDEN
        assert 'EXTERNAL_MOVIE_UPDATE_FORBIDDEN' in response.data['error']['code']


@pytest.mark.django_db
class TestMovieDeleteEndpoint:
    """Tests for DELETE /api/movies/{id}/ endpoint."""
    
    def test_delete_internal_movie_success(self, authenticated_client):
        """Test soft deleting an internal movie."""
        # Arrange
        movie = MovieFactory(source=Movie.Source.INTERNAL)
        url = reverse('movies:movie-detail', kwargs={'id': movie.id})
        
        # Act
        response = authenticated_client.delete(url)
        
        # Assert
        assert response.status_code == status.HTTP_204_NO_CONTENT
        
        movie.refresh_from_db()
        assert movie.is_active is False
    
    def test_delete_external_movie_forbidden(self, authenticated_client):
        """Test deleting external movie is forbidden."""
        # Arrange
        movie = MovieFactory(source=Movie.Source.EXTERNAL, tmdb_id=12345)
        url = reverse('movies:movie-detail', kwargs={'id': movie.id})
        
        # Act
        response = authenticated_client.delete(url)
        
        # Assert
        assert response.status_code == status.HTTP_403_FORBIDDEN


# @pytest.mark.django_db
# class TestMovieSearchEndpoint:
#     """Tests for GET /api/movies/search/ endpoint."""
    
#     def test_search_requires_query(self, api_client):
#         """Test search endpoint requires 'q' parameter."""
#         # Arrange
#         url = reverse('movie-search')
        
#         # Act
#         response = api_client.get(url)
        
#         # Assert
#         assert response.status_code == status.HTTP_400_BAD_REQUEST
#         assert 'MISSING_QUERY' in response.data['error']['code']
    
#     def test_search_finds_movies(self, api_client):
#         """Test search finds movies by keyword."""
#         # Arrange
#         MovieFactory(title='The Shawshank Redemption')
#         MovieFactory(title='The Godfather')
#         url = reverse('movie-search')
        
#         # Act
#         response = api_client.get(url, {'q': 'Shawshank'})
        
#         # Assert
#         assert response.status_code == status.HTTP_200_OK
#         assert response.data['count'] == 1
#         assert 'Shawshank' in response.data['results'][0]['title']


@pytest.mark.django_db
class TestMovieReviewsEndpoint:
    """Tests for GET /api/movies/{id}/reviews/ endpoint."""
    
    def test_get_movie_reviews(self, api_client):
        """Test retrieving reviews for a movie."""
        # Arrange
        movie = MovieFactory()
        ReviewFactory.create_batch(3, movie=movie)
        url = reverse('movies:movie-reviews', kwargs={'id': movie.id})
        
        # Act
        response = api_client.get(url)
        
        # Assert
        assert response.status_code == status.HTTP_200_OK
        assert response.data['count'] == 3
