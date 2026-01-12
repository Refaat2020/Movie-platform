"""
Custom filter backends for Movie API.

Uses django-filter for declarative filtering.
Provides clean separation between URL params and queryset filtering logic.
"""

from django_filters import rest_framework as filters
from rest_framework.exceptions import ValidationError

from django.db.models import Q

from src.apps.movies.models import Movie


class MovieFilter(filters.FilterSet):
    """
    Declarative filters for Movie model.
    """
    search = filters.CharFilter(
        method='filter_search',
        label='Search',
        help_text='Search in title, original_title, and overview'
    )

    min_rating = filters.NumberFilter(
        method='check_rating_range',
        field_name='vote_average',
        lookup_expr='gte',
        label='Minimum Rating',
        help_text='Minimum vote_average (0.0-10.0)'
    )
    max_rating = filters.NumberFilter(
        field_name='vote_average',
        lookup_expr='lte',
        label='Maximum Rating',
        help_text='Maximum vote_average (0.0-10.0)'
    )

    rating = filters.NumberFilter(
        field_name='vote_average',
        lookup_expr='exact',
        label='Exact Rating',
        help_text='Exact vote_average'
    )
 
    release_date_start = filters.DateFilter(
        # method='filter_year',
        field_name='release_date',
        lookup_expr='gte',
        label='Release Date Start',
        help_text='Start of release date range (YYYY-MM-DD)'
    )
    release_date_end = filters.DateFilter(
        # method='filter_year',
        field_name='release_date',
        lookup_expr='lte',
        label='Release Date End',
        help_text='End of release date range (YYYY-MM-DD)'
    )

    year = filters.NumberFilter(
        method='filter_year',
        field_name='release_date',
        lookup_expr='year',
        label='Release Year',
        help_text='Filter by release year'
    )

    genres = filters.CharFilter(
        method='filter_genres',
        label='Genres',
        help_text='Comma-separated genre slugs (e.g., "action,sci-fi")'
    )
    # source = filters.CharFilter(method='filter_source',field_name='source', choices=Movie.Source.choices,label='Source',help_text='Filter by movie source (external/internal)',)
    source = filters.ChoiceFilter(
        method='filter_source',
        field_name='source',
        choices=Movie.Source.choices,
        label='Source',
        help_text='Filter by movie source (external/internal)'
    )
 
    status = filters.ChoiceFilter(
        field_name='status',
        choices=Movie.Status.choices,
        label='Status',
        help_text='Filter by movie status'
    )
 
    min_popularity = filters.NumberFilter(
        field_name='popularity',
        lookup_expr='gte',
        label='Minimum Popularity',
        help_text='Minimum popularity score'
    )
    max_popularity = filters.NumberFilter(
        field_name='popularity',
        lookup_expr='lte',
        label='Maximum Popularity',
        help_text='Maximum popularity score'
    )

    is_active = filters.BooleanFilter(
        field_name='is_active',
        label='Is Active',
        help_text='Filter by active status (true/false)'
    )
    
    tmdb_id = filters.NumberFilter(
        field_name='tmdb_id',
        lookup_expr='exact',
        label='TMDB ID',
        help_text='Filter by TMDB ID'
    )
    
    class Meta:
        model = Movie
        fields = [
            'search',
            'min_rating',
            'max_rating',
            'rating',
            'release_date_start',
            'release_date_end',
            'year',
            'genres',
            'source',
            'status',
            'min_popularity',
            'max_popularity',
            'is_active',
            'tmdb_id',
        ]

    def filter_search(self, queryset, name, value):
        if not value:
            return queryset

        return queryset.filter(
            Q(title__icontains=value) |
            Q(original_title__icontains=value) |
            Q(overview__icontains=value)
        )
        
    def filter_source(self, queryset, name, value):
        if not value:
            return queryset
        return queryset.filter(source=value)

    def filter_genres(self, queryset, name, value):
        if not value:
            return queryset

        genre_slugs = [slug.strip() for slug in value.split(",") if slug.strip()]
        if not genre_slugs:
            return queryset

        for slug in genre_slugs:
            queryset = queryset.filter(genres__slug=slug)

        return queryset.distinct()


    def filter_year(self, queryset, name, value):
        if not value:
            return queryset

        year = value.year if hasattr(value, "year") else int(value)

        return queryset.filter(release_date__year=year)
    
    def check_rating_range(self,queryset,name,value):
        if value <0 or value >10:
            raise ValidationError("rating must be between 0.0 and 10.0")
        return queryset.filter(vote_average__gte=value)
        


class MovieOrderingFilter(filters.OrderingFilter):
    """
    Custom ordering filter with field validation.
    """
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.extra['choices'] = [
            ('popularity', 'Popularity (Ascending)'),
            ('-popularity', 'Popularity (Descending)'),
            ('release_date', 'Release Date (Ascending)'),
            ('-release_date', 'Release Date (Descending)'),
            ('vote_average', 'Rating (Ascending)'),
            ('-vote_average', 'Rating (Descending)'),
            ('vote_count', 'Vote Count (Ascending)'),
            ('-vote_count', 'Vote Count (Descending)'),
            ('created_at', 'Created At (Ascending)'),
            ('-created_at', 'Created At (Descending)'),
            ('title', 'Title (A-Z)'),
            ('-title', 'Title (Z-A)'),
        ]

    def filter(self, queryset, value):
        """
        Apply ordering with validation.
        """
        if not value:
            return queryset.order_by('-popularity', '-release_date', 'id')
        
        allowed_fields = [choice[0] for choice in self.extra['choices']]
        
        ordering_fields = []
        for field in value:
            if field not in allowed_fields:
                continue
            ordering_fields.append(field)
        
        if not ordering_fields:
            return queryset.order_by('-popularity', '-release_date', 'id')

        ordering_fields.append('id')
        
        return queryset.order_by(*ordering_fields)

def apply_movie_filters(queryset, query_params):
    """
    Apply all movie filters from query parameters.
    Returns filtered queryset or original on validation error.
    """
    filterset = MovieFilter(query_params, queryset=queryset)
    
    if not filterset.is_valid():
        return queryset
    
    return filterset.queryset
