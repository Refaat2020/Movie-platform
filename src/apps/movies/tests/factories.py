"""
Factory Boy factories for test data generation.

Provides:
- Realistic test data with sensible defaults
- Flexible customization for specific test scenarios
- Avoids duplication of test setup code
"""
import factory
from factory.django import DjangoModelFactory
from decimal import Decimal
from django.contrib.auth import get_user_model

from src.apps.movies.models import Movie, Genre, Review, IngestionLog


User = get_user_model()


class UserFactory(DjangoModelFactory):
    """Factory for creating test users."""
    
    class Meta:
        model = User
        django_get_or_create = ('username',)
    
    username = factory.Sequence(lambda n: f'user{n}')
    email = factory.LazyAttribute(lambda obj: f'{obj.username}@example.com')
    first_name = factory.Faker('first_name')
    last_name = factory.Faker('last_name')
    is_active = True
    is_staff = False


class GenreFactory(DjangoModelFactory):
    """Factory for creating test genres."""
    
    class Meta:
        model = Genre
        django_get_or_create = ('slug',)
    
    name = factory.Faker('word')
    slug = factory.LazyAttribute(lambda obj: obj.name.lower())
    tmdb_id = factory.Sequence(lambda n: 10000 + n)



class MovieFactory(DjangoModelFactory):
    """
    Factory for creating test movies.
    
    Usage:
        # Create internal movie
        movie = MovieFactory()
        
        # Create external (TMDB) movie
        movie = MovieFactory(source=Movie.Source.EXTERNAL, tmdb_id=12345)
        
        # Create with specific attributes
        movie = MovieFactory(
            title='Test Movie',
            vote_average=Decimal('8.5'),
            release_date='2024-01-01'
        )
        
        # Create batch
        movies = MovieFactory.create_batch(10)
    """
    
    class Meta:
        model = Movie
    
    # Core fields
    title = factory.Faker('catch_phrase')
    original_title = factory.LazyAttribute(lambda obj: obj.title)
    overview = factory.Faker('text', max_nb_chars=500)
    tagline = factory.Faker('sentence', nb_words=8)
    
    # Release info
    release_date = factory.Faker('date_between', start_date='-10y', end_date='today')
    runtime = factory.Faker('random_int', min=80, max=180)
    
    # Financial
    # budget = factory.Faker('random_int', min=1_000_000, max=200_000_000)
    # revenue = factory.Faker('random_int', min=500_000, max=1_000_000_000)
    # Metrics
    popularity = factory.Faker(
    'pydecimal',
    left_digits=2,
    right_digits=3,
    positive=True
)
    vote_average = factory.Faker(
    'pydecimal',
    left_digits=1,
    right_digits=1,
    positive=True,
    max_value=10
)
    vote_count = factory.Faker('random_int', min=1_000_000, max=200_000_000)
    
    # Media
    poster_path = factory.Faker('image_url', width=500, height=750)
    backdrop_path = factory.Faker('image_url', width=1920, height=1080)
    
    # Status
    status = Movie.Status.RELEASED
    source = Movie.Source.INTERNAL
    is_active = True
    
    # TMDB ID only for external movies
    tmdb_id = None
    
    @factory.post_generation
    def genres(self, create, extracted, **kwargs):
        """
        Add genres to movie after creation.
        
        Usage:
            # Create movie with specific genres
            action = GenreFactory(name='Action')
            movie = MovieFactory(genres=[action])
            
            # Create movie with auto-generated genres
            movie = MovieFactory(genres=3)  # Creates 3 random genres
        """
        if not create:
            return
        
        if extracted:
            # If a list of genres was passed, use it
            if isinstance(extracted, list):
                for genre in extracted:
                    self.genres.add(genre)
            # If an integer was passed, create that many genres
            elif isinstance(extracted, int):
                for _ in range(extracted):
                    self.genres.add(GenreFactory())
    


class ExternalMovieFactory(MovieFactory):
    """Convenience factory for external (TMDB) movies."""
    
    source = Movie.Source.EXTERNAL
    tmdb_id = factory.Sequence(lambda n: 100000 + n)


class ReviewFactory(DjangoModelFactory):
    """
    Factory for creating test reviews.
    
    Usage:
        # Create review for specific movie
        movie = MovieFactory()
        review = ReviewFactory(movie=movie)
        
        # Create review by specific user
        user = UserFactory()
        review = ReviewFactory(user=user)
        
        # Create batch of reviews for a movie
        reviews = ReviewFactory.create_batch(5, movie=movie)
    """
    
    class Meta:
        model = Review
    
    movie = factory.SubFactory(MovieFactory)
    user = factory.SubFactory(UserFactory)
    
    rating = factory.Faker(
        'pydecimal',
        left_digits=1,
        right_digits=1,
        positive=True,
        max_value=10
    )  
    title = factory.Faker('sentence', nb_words=6)
    content = factory.Faker('text', max_nb_chars=1000)
    # is_spoiler = factory.Faker('boolean', chance_of_getting_true=20)
    # helpful_count = factory.Faker('random_int', min=0, max=500)


class IngestionLogFactory(DjangoModelFactory):
    """Factory for creating test ingestion logs."""
    
    class Meta:
        model = IngestionLog
    
    job_type = IngestionLog.JobType.POPULAR_MOVIES
    status = IngestionLog.Status.COMPLETED
    
    movies_fetched = factory.Faker('random_int', min=1, max=100)
    movies_created = factory.Faker('random_int', min=0, max=50)
    movies_updated = factory.Faker('random_int', min=0, max=50)
    
    errors = factory.List([])
    metadata = factory.Dict({
        'api_endpoint': '/popular',
        'page': 1,
    })
    
    completed_at = factory.Faker('date_time_this_month')


# Convenience functions for common test scenarios

def create_popular_movies(count=10):
    """Create batch of popular movies (high vote_average and popularity)."""
    return MovieFactory.create_batch(
        count,
        vote_average=factory.Faker(
        'pydecimal',
         left_digits=1,
         right_digits=1,
         positive=True,
         max_value=10
        ),
        popularity=factory.Faker(
            'pydecimal',
            left_digits=2,
            right_digits=3,
            positive=True
        ),
        genres=2,
    )


def create_movie_with_reviews(review_count=5):
    """Create a movie with multiple reviews."""
    movie = MovieFactory(genres=2)
    ReviewFactory.create_batch(review_count, movie=movie)
    return movie


def create_genre_with_movies(movie_count=10):
    """Create a genre with multiple associated movies."""
    genre = GenreFactory()
    movies = MovieFactory.create_batch(movie_count)
    for movie in movies:
        movie.genres.add(genre)
    return genre, movies
