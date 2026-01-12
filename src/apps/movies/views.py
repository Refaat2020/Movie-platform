"""
Movie API Views using Django REST Framework.
Provides endpoints for listing, retrieving, creating,
"""
from rest_framework import viewsets, status
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticatedOrReadOnly
from django_filters.rest_framework import DjangoFilterBackend
from drf_spectacular.utils import extend_schema, extend_schema_view, OpenApiParameter
from drf_spectacular.types import OpenApiTypes
from rest_framework.filters import OrderingFilter
from rest_framework.permissions import IsAuthenticated , AllowAny
from rest_framework.authentication import BasicAuthentication

from drf_spectacular.openapi import AutoSchema
from src.apps.movies.models import Genre, Review
from src.apps.movies.serializers import (
    MovieListSerializer,
    MovieDetailSerializer,
    MovieCreateUpdateSerializer,
    GenreSerializer,
    ReviewSerializer,
)
from src.apps.movies.filters import MovieFilter, MovieOrderingFilter
from src.apps.movies.repositories.movie_repository import MovieRepository
from src.apps.common.pagination import StandardResultsSetPagination



@extend_schema_view(
    list=extend_schema(
        summary="List movies",
        description="Get paginated list of movies with filtering, search, and sorting",
        parameters=[
            OpenApiParameter(
                name='search',
                type=OpenApiTypes.STR,
                description='Search in title, original_title, and overview'
            ),
            OpenApiParameter(
                name='min_rating',
                type=OpenApiTypes.DECIMAL,
                description='Minimum vote_average (0.0-10.0)'
            ),
            OpenApiParameter(
                name='max_rating',
                type=OpenApiTypes.DECIMAL,
                description='Maximum vote_average (0.0-10.0)'
            ),
            OpenApiParameter(
                name='release_date_start',
                type=OpenApiTypes.DATE,
                description='Start of release date range (YYYY-MM-DD)'
            ),
            OpenApiParameter(
                name='release_date_end',
                type=OpenApiTypes.DATE,
                description='End of release date range (YYYY-MM-DD)'
            ),
            OpenApiParameter(
                name='genres',
                type=OpenApiTypes.STR,
                description='Comma-separated genre slugs (AND logic)'
            ),
            OpenApiParameter(
                name='source',
                type=OpenApiTypes.STR,
                description='Filter by source: external or internal',
                enum=['external', 'internal']
            ),
            OpenApiParameter(
                name='ordering',
                type=OpenApiTypes.STR,
                description='Sort by field. Prefix with - for descending. Options: popularity, release_date, vote_average, created_at, title',
                default='-popularity'
            ),
        ]
    ),
    retrieve=extend_schema(
        summary="Get movie details",
        description="Get detailed information about a specific movie"
    ),
    create=extend_schema(
        summary="Create movie",
        description="Create a new internal (user-created) movie"
    ),
    update=extend_schema(
        summary="Update movie",
        description="Update an existing movie (internal movies only)"
    ),
    partial_update=extend_schema(
        summary="Partially update movie",
        description="Partially update an existing movie (internal movies only)"
    ),
    destroy=extend_schema(
        summary="Delete movie",
        description="Soft delete a movie (sets is_active=False)"
    ),
)
class MovieViewSet(viewsets.ModelViewSet):
    """
    ViewSet for Movie CRUD operations.
    """
    
    # permission_classes = [IsAuthenticatedOrReadOnly]
    authentication_classes = [BasicAuthentication]
    pagination_class = StandardResultsSetPagination
    filter_backends = [DjangoFilterBackend, OrderingFilter]
    filterset_class = MovieFilter
    lookup_field = 'id'
    
    def get_permissions(self):
        if self.action in ["create", "update", "partial_update", "destroy"]:
            return [IsAuthenticated()]
        return [AllowAny()]

    def get_queryset(self):
        """
        Get optimized queryset from repository.
        """
        if self.action == 'list':
            # Lighter queryset for list view
            return MovieRepository.get_active_movies()
        return MovieRepository.get_optimized_queryset().filter(is_active=True)

    def get_serializer_class(self):
        """
        Return appropriate serializer based on action.
        """
        if self.action == 'list':
            return MovieListSerializer
        elif self.action in ['create', 'update', 'partial_update']:
            return MovieCreateUpdateSerializer
        return MovieDetailSerializer

    def create(self, request, *args, **kwargs):
        """
        Create a new movie.

        Status Codes:
        201 = Created successfully
        400 = Validation error
        401 = Unauthorized
        """
        serializer = self.get_serializer(data=request.data)

        if not serializer.is_valid():
            return Response(
                {
                    "error": {
                        "details": serializer.errors
                    }
                },
                status=status.HTTP_400_BAD_REQUEST
            )

        movie = serializer.save()
        detail_serializer = MovieDetailSerializer(movie)

        return Response(
            detail_serializer.data,
            status=status.HTTP_201_CREATED
        )



    def update(self, request, *args, **kwargs):
        """
        Update a movie.
        
        Status Codes:
        200= Updated successfully
        400= Validation error
        403= Forbidden 
        404= Movie not found
        """
        instance = self.get_object()
        
        # Check if movie is internal (user-created)
        if instance.is_external:
            return Response(
                {
                    'error': {
                        'code': 'EXTERNAL_MOVIE_UPDATE_FORBIDDEN',
                        'message': 'Cannot update external movies from TMDB',
                        'details': {
                            'movie_id': str(instance.id),
                            'tmdb_id': instance.tmdb_id,
                            'source': instance.source,
                        }
                    }
                },
                status=status.HTTP_403_FORBIDDEN
            )
        
        partial = kwargs.pop('partial', False)
        serializer = self.get_serializer(instance, data=request.data, partial=partial)
        serializer.is_valid(raise_exception=True)
        
        movie = serializer.save()
        
        # Return full details of updated movie
        detail_serializer = MovieDetailSerializer(movie)
        
        return Response(detail_serializer.data)

    def destroy(self, request, *args, **kwargs):
        """
        Soft delete a movie.
    
        Status Codes:
        204= Deleted successfully
        403= Forbidden (trying to delete external movie)
        404= Movie not found
        """
        instance = self.get_object()
        
        if instance.is_external:
            return Response(
                {
                    'error': {
                        'code': 'EXTERNAL_MOVIE_DELETE_FORBIDDEN',
                        'message': 'Cannot delete external movies from TMDB',
                        'details': {
                            'movie_id': str(instance.id),
                            'tmdb_id': instance.tmdb_id,
                            'source': instance.source,
                        }
                    }
                },
                status=status.HTTP_403_FORBIDDEN
            )

        success = MovieRepository.soft_delete(str(instance.id))
        
        if not success:
            return Response(
                {
                    'error': {
                        'code': 'DELETE_FAILED',
                        'message': 'Failed to delete movie',
                    }
                },
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
        
        return Response(status=status.HTTP_204_NO_CONTENT)

    @extend_schema(
        summary="Get movie reviews",
        description="Get all reviews for a specific movie"
    )
    @action(detail=True, methods=['get'])
    def reviews(self, request, *args, **kwargs):
        """
        Get reviews for a specific movie.
        
        """
        movie = self.get_object()
        reviews = movie.reviews.select_related('user').order_by('-created_at')
 
        page = self.paginate_queryset(reviews)
        if page is not None:
            serializer = ReviewSerializer(page, many=True)
            return self.get_paginated_response(serializer.data)
        
        serializer = ReviewSerializer(reviews, many=True)
        return Response(serializer.data)

    @extend_schema(
        summary="Search movies",
        description="Full-text search across movie titles and descriptions",
        parameters=[
            OpenApiParameter(
                name='q',
                type=OpenApiTypes.STR,
                required=True,
                description='Search query'
            ),
        ]
    )
    @action(detail=False, methods=['get'])
    def search(self, request):
        """
        Full-text search endpoint.
        """
        query = request.query_params.get('q', '').strip()
        
        if not query:
            return Response(
                {
                    'error': {
                        'code': 'MISSING_QUERY',
                        'message': 'Search query parameter "q" is required',
                    }
                },
                status=status.HTTP_400_BAD_REQUEST
            )
        movies = MovieRepository.search_by_keyword(query)

        page = self.paginate_queryset(movies)
        if page is not None:
            serializer = MovieListSerializer(page, many=True)
            return self.get_paginated_response(serializer.data)
        
        serializer = MovieListSerializer(movies, many=True)
        return Response(serializer.data)


@extend_schema_view(
    list=extend_schema(
        summary="List genres",
        description="Get all available movie genres"
    ),
    retrieve=extend_schema(
        summary="Get genre details",
        description="Get details of a specific genre including movie count"
    ),
)
class GenreViewSet(viewsets.ReadOnlyModelViewSet):
    """
    ViewSet for Genre read-only operations.
    """
    
    queryset = Genre.objects.all()
    serializer_class = GenreSerializer
    permission_classes = [] 
    pagination_class = None  

    @extend_schema(
        summary="Get movies in genre",
        description="Get all movies that belong to this genre"
    )
    @action(detail=True, methods=['get'])
    def movies(self, request):
        """
        Get movies for a specific genre.
        """
        genre = self.get_object()
        movies = MovieRepository.get_active_movies().filter(genres=genre)
        paginator = StandardResultsSetPagination()
        page = paginator.paginate_queryset(movies, request)
        
        if page is not None:
            serializer = MovieListSerializer(page, many=True)
            return paginator.get_paginated_response(serializer.data)
        
        serializer = MovieListSerializer(movies, many=True)
        return Response(serializer.data)


@extend_schema_view(
    list=extend_schema(
        summary="List reviews",
        description="Get paginated list of all reviews"
    ),
    retrieve=extend_schema(
        summary="Get review details",
        description="Get details of a specific review"
    ),
    create=extend_schema(
        summary="Create review",
        description="Create a new review for a movie (authenticated users only)"
    ),
    update=extend_schema(
        summary="Update review",
        description="Update your own review"
    ),
    partial_update=extend_schema(
        summary="Partially update review",
        description="Partially update your own review"
    ),
    destroy=extend_schema(
        summary="Delete review",
        description="Delete your own review"
    ),
)
class ReviewViewSet(viewsets.ModelViewSet):
    """
    ViewSet for Review CRUD operations.
    """
    
    serializer_class = ReviewSerializer
    permission_classes = [IsAuthenticatedOrReadOnly]
    pagination_class = StandardResultsSetPagination
    
    def get_queryset(self):
        """Get reviews with optimized queries."""
        return Review.objects.select_related('user', 'movie').order_by('-created_at')

    def perform_create(self, serializer):
        """Create review with current user."""
        serializer.save(user=self.request.user)

    def perform_update(self, serializer):
        """Ensure user can only update their own reviews."""
        if serializer.instance.user != self.request.user:
            raise PermissionError("You can only update your own reviews")
        serializer.save()

    def perform_destroy(self, instance):
        """Ensure user can only delete their own reviews."""
        if instance.user != self.request.user:
            raise PermissionError("You can only delete your own reviews")
        instance.delete()


def get_permissions(self):
    if self.action in ["create", "update", "partial_update", "destroy"]:
        return [IsAuthenticated()]
    return []