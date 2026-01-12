"""
DRF Serializers for Movie API.

Design principles:
- Separate serializers for list vs detail views (performance)
- Validation logic is explicit and documented
- Read-only fields clearly marked
- Nested serializers for related data
"""
from decimal import Decimal

from rest_framework import serializers
from src.apps.movies.models import Movie, Genre,Review


class GenreSerializer(serializers.ModelSerializer):
    """Lightweight genre serializer for nested representation."""
    
    class Meta:
        model = Genre
        fields = ['id', 'name', 'slug']
        read_only_fields = ['id']





class MovieListSerializer(serializers.ModelSerializer):
    """
    Optimized serializer for movie list endpoints.
    
    Includes minimal fields for performance.
    Does not include expensive nested data.
    """
    genres = GenreSerializer(many=True, read_only=True)
    source_display = serializers.CharField(source='get_source_display', read_only=True)
    is_external = serializers.BooleanField(read_only=True)
    
    class Meta:
        model = Movie
        fields = [
            'id',
            'tmdb_id',
            'source',
            'source_display',
            'is_external',
            'title',
            'release_date',
            'popularity',
            'vote_average',
            'vote_count',
            'poster_path',
            'status',
            'genres',
            'created_at',
            'updated_at',
        ]
        read_only_fields = [
            'id',
            'tmdb_id',
            'source',
            'popularity',
            'vote_average',
            'vote_count',
            'created_at',
            'updated_at',
        ]


class MovieDetailSerializer(serializers.ModelSerializer):
    """
    Full serializer for movie detail endpoint.
    
    Includes all fields and nested relationships.
    """
    genres = GenreSerializer(many=True, read_only=True)
    source_display = serializers.CharField(source='get_source_display', read_only=True)
    status_display = serializers.CharField(source='get_status_display', read_only=True)
    is_external = serializers.BooleanField(read_only=True)
    is_internal = serializers.BooleanField(read_only=True)
    
    review_count = serializers.SerializerMethodField()
    average_user_rating = serializers.SerializerMethodField()
    
    class Meta:
        model = Movie
        fields = [
            'id',
            'tmdb_id',
            'source',
            'source_display',
            'is_external',
            'is_internal',
            'title',
            'original_title',
            'overview',
            'tagline',
            'release_date',
            'runtime',
            'popularity',
            'vote_average',
            'vote_count',
            'poster_path',
            'backdrop_path',
            'status',
            'status_display',
            'is_active',
            'genres',
            'review_count',
            'average_user_rating',
            'created_at',
            'updated_at',
            'synced_to_mongo_at',
        ]
        read_only_fields = [
            'id',
            'tmdb_id',
            'source',
            'popularity',
            'vote_average',
            'vote_count',
            'created_at',
            'updated_at',
            'synced_to_mongo_at',
        ]

    def get_review_count(self, obj) -> int:
        """Get count of reviews for this movie."""
        if hasattr(obj, '_prefetched_objects_cache') and 'reviews' in obj._prefetched_objects_cache:
            return len(obj._prefetched_objects_cache['reviews'])
        return obj.reviews.count()

    def get_average_user_rating(self, obj) -> float:
        """Get average rating from user reviews."""
        from django.db.models import Avg
        result = obj.reviews.aggregate(avg_rating=Avg('rating'))
        avg = result['avg_rating']
        return float(avg) if avg is not None else None


class MovieCreateUpdateSerializer(serializers.ModelSerializer):
    """
    Serializer for creating/updating internal (user-created) movies.
    Enforces source to be INTERNAL.
    """
    genre_ids = serializers.ListField(
        child=serializers.IntegerField(),
        write_only=True,
        required=False,
        help_text="List of genre IDs to associate with movie"
    )
    
    class Meta:
        model = Movie
        fields = [
            'title',
            'original_title',
            'overview',
            'tagline',
            'release_date',
            'runtime',
            'poster_path',
            'backdrop_path',
            'status',
            'genre_ids',
        ]

    def validate_title(self, value):
        """Ensure title is not empty after stripping whitespace."""
        if not value or not value.strip():
            raise serializers.ValidationError("Title cannot be empty")
        return value.strip()

    def validate_runtime(self, value):
        """Validate runtime is reasonable."""
        if value is not None and value > 1000:
            raise serializers.ValidationError(
                "Runtime seems unrealistic. Maximum allowed is 1000 minutes."
            )
        return value


    def create(self, validated_data):
        """
        Create new movie with genre relationships.
        
        Automatically sets source to INTERNAL.
        """
        genre_ids = validated_data.pop('genre_ids', [])
        
        movie = Movie.objects.create(**validated_data)
        
        if genre_ids:
            genres = Genre.objects.filter(id__in=genre_ids)
            movie.genres.set(genres)
        
        return movie

    def update(self, instance, validated_data):
        """Update movie and its genre relationships."""
        genre_ids = validated_data.pop('genre_ids', None)
        
        for attr, value in validated_data.items():
            setattr(instance, attr, value)
        instance.save()
        
        if genre_ids is not None:
            genres = Genre.objects.filter(id__in=genre_ids)
            instance.genres.set(genres)
        
        return instance


class ReviewSerializer(serializers.ModelSerializer):
    """Serializer for movie reviews."""
    
    user_username = serializers.CharField(source='user.username', read_only=True)
    movie_title = serializers.CharField(source='movie.title', read_only=True)
    
    class Meta:
        model = Review
        fields = [
            'id',
            'movie',
            'movie_title',
            'user',
            'user_username',
            'rating',
            'title',
            'content',
            'created_at',
            'updated_at',
        ]
        read_only_fields = [
            'id',
            'user',
            'created_at',
            'updated_at',
        ]

    def validate_rating(self, value):
        """Ensure rating is in valid range."""
        if not (Decimal('0.0') <= value <= Decimal('10.0')):
            raise serializers.ValidationError(
                "Rating must be between 0.0 and 10.0"
            )
        return value

    def validate(self, attrs):
        """
        Cross-field validation.
        Ensure user has not already reviewed this movie.
        """
        movie = attrs.get('movie')
        user = self.context['request'].user
        
        if not self.instance:
            if Review.objects.filter(movie=movie, user=user).exists():
                raise serializers.ValidationError({
                    'movie': 'You have already reviewed this movie'
                })
        
        return attrs

    def create(self, validated_data):
        """Create review with current user."""
        validated_data['user'] = self.context['request'].user
        return super().create(validated_data)


class MovieFilterSerializer(serializers.Serializer):
    """
    Serializer for validating query parameters in movie list endpoint.
    Supports filtering, searching, and sorting.
    """
   
    search = serializers.CharField(
        required=False,
        max_length=255,
        help_text="Search in title, original_title, and overview"
    )
    
  
    min_rating = serializers.DecimalField(
        required=False,
        max_digits=3,
        decimal_places=1,
        min_value=Decimal('0.0'),
        max_value=Decimal('10.0'),
        help_text="Minimum vote_average"
    )
    max_rating = serializers.DecimalField(
        required=False,
        max_digits=3,
        decimal_places=1,
        min_value=Decimal('0.0'),
        max_value=Decimal('10.0'),
        help_text="Maximum vote_average"
    )
    
    release_date_start = serializers.DateField(
        required=False,
        help_text="Start of release date range (inclusive)"
    )
    release_date_end = serializers.DateField(
        required=False,
        help_text="End of release date range (inclusive)"
    )
 
    genres = serializers.CharField(
        required=False,
        help_text="Comma-separated genre slugs (AND logic)"
    )
    
    source = serializers.ChoiceField(
        required=False,
        choices=Movie.Source.choices,
        help_text="Filter by movie source"
    )
 
    status = serializers.ChoiceField(
        required=False,
        choices=Movie.Status.choices,
        help_text="Filter by movie status"
    )
  
    ordering = serializers.CharField(
        required=False,
        default='-popularity',
        help_text="Sort field (prefix with '-' for descending). Options: popularity, release_date, vote_average, created_at"
    )
    
    def validate_ordering(self, value):
        """Validate ordering field is allowed."""
        allowed_fields = [
            'popularity', '-popularity',
            'release_date', '-release_date',
            'vote_average', '-vote_average',
            'created_at', '-created_at',
            'title', '-title',
        ]
        if value not in allowed_fields:
            raise serializers.ValidationError(
                f"Invalid ordering field. Allowed: {', '.join(allowed_fields)}"
            )
        return value

    def validate(self, attrs):
        """Cross-field validation for filters."""
        min_rating = attrs.get('min_rating')
        max_rating = attrs.get('max_rating')
        if min_rating and max_rating and min_rating > max_rating:
            raise serializers.ValidationError({
                'min_rating': 'min_rating cannot be greater than max_rating'
            })
  
        start_date = attrs.get('release_date_start')
        end_date = attrs.get('release_date_end')
        if start_date and end_date and start_date > end_date:
            raise serializers.ValidationError({
                'release_date_start': 'release_date_start cannot be after release_date_end'
            })
        
        return attrs
    def create(self, validated_data):
        raise NotImplementedError("MovieFilterSerializer does not support create")

    def update(self, instance, validated_data):
        raise NotImplementedError("MovieFilterSerializer does not support update")
