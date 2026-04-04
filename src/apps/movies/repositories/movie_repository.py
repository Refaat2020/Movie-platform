"""
Movie repository pattern implementation.
Encapsulates all data access logic for Movie model.
"""
from decimal import Decimal
from typing import Any, Dict, Optional

from django.db import transaction
from django.db.models import Prefetch, Q, QuerySet
from django.db import models
from src.apps.movies.models import Genre, Movie


class MovieRepository:
    """
    Repository for Movie model data access.
    Design principles:
    - All queries return QuerySets (for lazy evaluation and chaining)
    - Optimizations (select_related, prefetch_related) are encapsulated here
    - Methods are atomic where appropriate
    """

    @staticmethod
    def get_optimized_queryset() -> QuerySet:
        """
        Return optimized queryset for movies with related data.
        """
        return Movie.objects.select_related().prefetch_related(
            'genres',
            Prefetch(
                'reviews',
                queryset=None  # Will be filtered in views if needed
            )
        )

    @staticmethod
    def get_active_movies() -> QuerySet:
        """Get only active (non-soft-deleted) movies."""
        return (Movie.objects.filter(is_active=True).select_related().prefetch_related('genres'))

    @staticmethod
    def get_by_id(movie_id: str) -> Optional[Movie]:
        """
        Get single movie by UUID with optimizations.
        """
        try:
            return MovieRepository.get_optimized_queryset().get(id=movie_id)
        except Movie.DoesNotExist:
            return None

    @staticmethod
    def get_by_tmdb_id(tmdb_id: int) -> Optional[Movie]:
        """
        Get movie by TMDB ID.
        """
        try:
            return Movie.objects.get(tmdb_id=tmdb_id)
        except Movie.DoesNotExist:
            return None

    @staticmethod
    def search_by_keyword(keyword: str, queryset: Optional[QuerySet] = None) -> QuerySet:
        """
        Full-text search on title, original_title, and overview.
        """
        if queryset is None:
            queryset = MovieRepository.get_active_movies()

        search_query = Q(search_vector__isnull=False)
        results = queryset.filter(search_query).filter(
            search_vector=keyword
        )
        if not results.exists():
            results = queryset.filter(
                Q(title__icontains=keyword) |
                Q(original_title__icontains=keyword) |
                Q(overview__icontains=keyword)
            )

        return results

    @staticmethod
    def filter_by_rating(
        min_rating: Optional[Decimal] = None,
        max_rating: Optional[Decimal] = None,
        queryset: Optional[QuerySet] = None
    ) -> QuerySet:
        """Filter movies by vote_average range."""
        if queryset is None:
            queryset = MovieRepository.get_active_movies()

        if min_rating is not None:
            queryset = queryset.filter(vote_average__gte=min_rating)
        if max_rating is not None:
            queryset = queryset.filter(vote_average__lte=max_rating)

        return queryset

    @staticmethod
    def filter_by_release_date(
        start_date=None,
        end_date=None,
        queryset: Optional[QuerySet] = None
    ) -> QuerySet:
        """Filter movies by release_date range."""
        if queryset is None:
            queryset = MovieRepository.get_active_movies()

        if start_date is not None:
            queryset = queryset.filter(release_date__gte=start_date)
        if end_date is not None:
            queryset = queryset.filter(release_date__lte=end_date)

        return queryset

    @staticmethod
    def filter_by_genres(genre_slugs: list[str], queryset: Optional[QuerySet] = None) -> QuerySet:
        """
        Filter movies by genre slugs
        """
        if not genre_slugs:
            return queryset or MovieRepository.get_active_movies()

        if queryset is None:
            queryset = MovieRepository.get_active_movies()

        for slug in genre_slugs:
            queryset = queryset.filter(genres__slug=slug)

        return queryset.distinct()

    @staticmethod
    def filter_by_source(source: str, queryset: Optional[QuerySet] = None) -> QuerySet:
        """Filter by source (external/internal)."""
        if queryset is None:
            queryset = MovieRepository.get_active_movies()

        return queryset.filter(source=source)

    @staticmethod
    @transaction.atomic
    def upsert_from_tmdb(tmdb_data: Dict[str, Any]) -> tuple[Movie, bool]:
        """
        Idempotent upsert operation for TMDB movies.
        Args:
            tmdb_data: Dict containing TMDB movie data
        Returns:
            Tuple of (Movie instance, created: bool)
        """
        tmdb_id = tmdb_data.get('id')
        if not tmdb_id:
            raise ValueError("tmdb_data must include 'id' field")

        genre_ids = tmdb_data.pop('genre_ids', [])
        genres_data = tmdb_data.pop('genres', [])

        defaults = {
            'title': tmdb_data.get('title', ''),
            'original_title': tmdb_data.get('original_title', ''),
            'overview': tmdb_data.get('overview', ''),
            'tagline': tmdb_data.get('tagline', ''),
            'release_date': tmdb_data.get('release_date'),
            'runtime': tmdb_data.get('runtime'),
            'popularity': Decimal(str(tmdb_data.get('popularity', 0))),
            'vote_average': Decimal(str(tmdb_data.get('vote_average', 0))),
            'vote_count': tmdb_data.get('vote_count', 0),
            'poster_path': tmdb_data.get('poster_path', ''),
            'backdrop_path': tmdb_data.get('backdrop_path', ''),
            'status': tmdb_data.get('status', Movie.Status.RELEASED).lower().replace(' ', '_'),
            'source': Movie.Source.EXTERNAL,
            'is_active': True,
        }

        movie, created = Movie.objects.update_or_create(
            tmdb_id=tmdb_id,
            defaults=defaults
        )
        if genre_ids or genres_data:
            MovieRepository._sync_genres(movie, genre_ids, genres_data)

        return movie, created

    @staticmethod
    def _sync_genres(movie: Movie, genre_ids: list[int], genres_data: list[dict]):
        """
        Sync genres for a movie from TMDB data.
        Args:
            movie: Movie instance to update
            genre_ids: List of TMDB genre IDs
            genres_data: List of genre dicts from TMDB
        """
        genre_objects = []

        if genres_data:
            for genre_data in genres_data:
                genre, _ = Genre.objects.get_or_create(
                    tmdb_id=genre_data['id'],
                    defaults={
                        'name': genre_data['name'],
                        'slug': genre_data['name'].lower().replace(' ', '-')
                    }
                )
                genre_objects.append(genre)

        elif genre_ids:
            genre_objects = list(Genre.objects.filter(tmdb_id__in=genre_ids))

        if genre_objects:
            movie.genres.set(genre_objects)

    @staticmethod
    def soft_delete(movie_id: str) -> bool:
        """
        Soft delete a movie 
        by setting is_active to False.
        """
        try:
            movie = Movie.objects.get(id=movie_id)
            movie.is_active = False
            movie.save(update_fields=['is_active', 'updated_at'])
            return True
        except Movie.DoesNotExist:
            return False

    @staticmethod
    def get_movies_needing_sync(limit: int = 100) -> QuerySet:
        """
        Get movies that haven't been synced to MongoDB recently.
        Used for background sync tasks.
        """

        return Movie.objects.filter(
            Q(synced_to_mongo_at__isnull=True) |
            Q(synced_to_mongo_at__lt=models.F('updated_at'))
        ).order_by('-updated_at')[:limit]
