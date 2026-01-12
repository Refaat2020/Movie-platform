"""
Django Admin configuration for Movie models.

Provides:
- Searchable, filterable admin interfaces
- Inline editing for relationships
- Actions for bulk operations
- Read-only fields for computed/synced data
"""
from django.contrib import admin
from django.utils.html import format_html
from django.urls import reverse
from django.utils.safestring import mark_safe

from src.apps.movies.models import Movie, Genre, Review, IngestionLog


@admin.register(Genre)
class GenreAdmin(admin.ModelAdmin):
    """Admin interface for Genre model."""
    
    list_display = ['name', 'slug', 'tmdb_id', 'movie_count', 'created_at']
    search_fields = ['name', 'slug']
    list_filter = ['created_at']
    readonly_fields = ['created_at', 'updated_at']
    prepopulated_fields = {'slug': ('name',)}
    
    def movie_count(self, obj):
        """Display count of movies in this genre."""
        count = obj.movies.count()
        url = reverse('admin:movies_movie_changelist') + f'?genres__id__exact={obj.id}'
        return format_html('<a href="{}">{} movies</a>', url, count)
    movie_count.short_description = 'Movies'




class ReviewInline(admin.TabularInline):
    """Inline editor for reviews on movie detail page."""
    model = Review
    extra = 0
    readonly_fields = ['user', 'created_at']
    fields = ['user', 'rating', 'title', 'created_at']
    can_delete = True


@admin.register(Movie)
class MovieAdmin(admin.ModelAdmin):
    """Admin interface for Movie model."""
    
    list_display = [
        'title',
        # 'source_badge',
        'release_date',
        'vote_average',
        'popularity',
        'status',
        'is_active',
        # 'sync_status',
    ]
    list_filter = [
        'source',
        'status',
        'is_active',
        'release_date',
        'genres',
        'created_at',
    ]
    search_fields = ['title', 'original_title', 'overview', 'tmdb_id']
    readonly_fields = [
        'id',
        'tmdb_id',
        'source',
        'popularity',
        'vote_average',
        'vote_count',
        'created_at',
        'updated_at',
        'synced_to_mongo_at',
        'poster_preview',
    ]
    filter_horizontal = ['genres']
    date_hierarchy = 'release_date'
    inlines = [ReviewInline]
    
    fieldsets = (
        ('Identification', {
            'fields': ('id', 'tmdb_id', 'source')
        }),
        ('Basic Information', {
            'fields': ('title', 'original_title', 'tagline', 'overview')
        }),
        ('Release & Status', {
            'fields': ('release_date', 'runtime', 'status', 'is_active')
        }),
        # ('Financial', {
        #     'fields': ('budget', 'revenue'),
        #     'classes': ('collapse',)
        # }),
        ('Metrics', {
            'fields': ('popularity', 'vote_average', 'vote_count'),
        }),
        ('Media', {
            'fields': ('poster_path', 'poster_preview', 'backdrop_path'),
        }),
        ('Relationships', {
            'fields': ('genres',),
        }),
        ('System', {
            'fields': ('created_at', 'updated_at', 'synced_to_mongo_at'),
            'classes': ('collapse',)
        }),
    )
    
    actions = ['mark_active', 'mark_inactive', 'trigger_mongo_sync']
    
    def source_badge(self, obj):
        """Display source with colored badge."""
        if obj.is_external:
            return format_html(
                '<span style="background-color: #2196F3; color: white; padding: 3px 8px; '
                'border-radius: 3px;">TMDB</span>'
            )
        return format_html(
            '<span style="background-color: #4CAF50; color: white; padding: 3px 8px; '
            'border-radius: 3px;">USER</span>'
        )
    source_badge.short_description = 'Source'
    
    def sync_status(self, obj):
        """Display MongoDB sync status."""
        if not obj.synced_to_mongo_at:
            return format_html('<span style="color: orange;">⚠ Not Synced</span>')
        
        if obj.synced_to_mongo_at < obj.updated_at:
            return format_html('<span style="color: orange;">⚠ Out of Sync</span>')
        
        return format_html('<span style="color: green;">✓ Synced</span>')
    sync_status.short_description = 'MongoDB Sync'
    
    def poster_preview(self, obj):
        """Display poster image preview."""
        if obj.poster_path:
            return format_html(
                '<img src="{}" style="max-width: 200px; max-height: 300px;" />',
                obj.poster_path
            )
        return '-'
    poster_preview.short_description = 'Poster Preview'
    
    def mark_active(self, request, queryset):
        """Bulk action to activate movies."""
        count = queryset.update(is_active=True)
        self.message_user(request, f'{count} movie(s) marked as active.')
    mark_active.short_description = 'Mark selected movies as active'
    
    def mark_inactive(self, request, queryset):
        """Bulk action to deactivate movies."""
        count = queryset.update(is_active=False)
        self.message_user(request, f'{count} movie(s) marked as inactive.')
    mark_inactive.short_description = 'Mark selected movies as inactive'
    
    def trigger_mongo_sync(self, request, queryset):
        """Bulk action to trigger MongoDB sync."""
        from apps.integrations.mongodb.tasks import sync_movies_to_mongodb
        
        movie_ids = [str(movie.id) for movie in queryset]
        sync_movies_to_mongodb.delay(movie_ids)
        
        self.message_user(
            request,
            f'Triggered MongoDB sync for {len(movie_ids)} movie(s). '
            'Check Celery logs for progress.'
        )
    trigger_mongo_sync.short_description = 'Sync selected movies to MongoDB'


@admin.register(Review)
class ReviewAdmin(admin.ModelAdmin):
    """Admin interface for Review model."""
    
    list_display = [
        'title',
        'movie_link',
        'user',
        'rating',
        'created_at',
    ]
    list_filter = ['rating', 'created_at']
    search_fields = ['title', 'content', 'movie__title', 'user__username']
    readonly_fields = ['id', 'created_at', 'updated_at']
    date_hierarchy = 'created_at'
    
    fieldsets = (
        ('Review', {
            'fields': ('movie', 'user', 'rating', 'title', 'content',)
        }),
       
        ('System', {
            'fields': ('id', 'created_at', 'updated_at'),
            'classes': ('collapse',)
        }),
    )
    
    def movie_link(self, obj):
        """Link to movie admin page."""
        url = reverse('admin:movies_movie_change', args=[obj.movie.id])
        return format_html('<a href="{}">{}</a>', url, obj.movie.title)
    movie_link.short_description = 'Movie'


@admin.register(IngestionLog)
class IngestionLogAdmin(admin.ModelAdmin):
    """Admin interface for IngestionLog model."""
    
    list_display = [
        'job_type',
        'status_badge',
        'started_at',
        'duration',
        'movies_fetched',
        'movies_created',
        'movies_updated',
        'error_count',
    ]
    list_filter = ['job_type', 'status', 'started_at']
    search_fields = ['metadata', 'errors']
    readonly_fields = [
        'id',
        'started_at',
        'completed_at',
        'duration',
        'formatted_errors',
        'formatted_metadata',
    ]
    date_hierarchy = 'started_at'
    
    fieldsets = (
        ('Job Information', {
            'fields': ('id', 'job_type', 'status')
        }),
        ('Timing', {
            'fields': ('started_at', 'completed_at', 'duration')
        }),
        ('Statistics', {
            'fields': ('movies_fetched', 'movies_created', 'movies_updated')
        }),
        ('Details', {
            'fields': ('formatted_errors', 'formatted_metadata'),
        }),
    )
    
    def status_badge(self, obj):
        """Display status with colored badge."""
        colors = {
            'started': '#2196F3',
            'completed': '#4CAF50',
            'failed': '#F44336',
            'partial': '#FF9800',
        }
        color = colors.get(obj.status, '#9E9E9E')
        return format_html(
            '<span style="background-color: {}; color: white; padding: 3px 8px; '
            'border-radius: 3px;">{}</span>',
            color,
            obj.get_status_display()
        )
    status_badge.short_description = 'Status'
    
    def duration(self, obj):
        """Display job duration."""
        if obj.duration_seconds:
            return f'{obj.duration_seconds:.2f}s'
        return '-'
    duration.short_description = 'Duration'
    
    def error_count(self, obj):
        """Display error count."""
        count = len(obj.errors) if obj.errors else 0
        if count > 0:
            return format_html('<span style="color: red;">{} errors</span>', count)
        return '0'
    error_count.short_description = 'Errors'
    
    def formatted_errors(self, obj):
        """Display errors in readable format."""
        if not obj.errors:
            return '-'
        
        errors_html = '<ul>'
        for error in obj.errors:
            errors_html += f'<li>{error}</li>'
        errors_html += '</ul>'
        
        return mark_safe(errors_html)
    formatted_errors.short_description = 'Error Messages'
    
    def formatted_metadata(self, obj):
        """Display metadata in readable format."""
        if not obj.metadata:
            return '-'
        
        import json
        return mark_safe(
            f'<pre>{json.dumps(obj.metadata, indent=2)}</pre>'
        )
    formatted_metadata.short_description = 'Metadata'
    
    def has_add_permission(self, request):
        """Prevent manual creation of ingestion logs."""
        return False
