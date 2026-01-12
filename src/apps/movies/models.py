"""
Movie domain models.
"""
import uuid
from decimal import Decimal
from django.db import models
from django.core.validators import MinValueValidator, MaxValueValidator
from django.contrib.postgres.indexes import GinIndex
from django.contrib.postgres.search import SearchVectorField, SearchVector
from django.db import connection


class TimestampedModel(models.Model):
    """Abstract base model with timestamp tracking."""
    created_at = models.DateTimeField(auto_now_add=True, db_index=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        abstract = True


class Genre(TimestampedModel):
    """Movie genre classification."""
    
    tmdb_id = models.IntegerField(
        unique=True,
        null=True,
        blank=True,
        db_index=True,
        help_text="External ID from TheMovieDB"
    )
    name = models.CharField(max_length=100, unique=True, db_index=True)
    slug = models.SlugField(unique=True, db_index=True)

    class Meta:
        db_table = 'genres'
        ordering = ['name']
        verbose_name = 'Genre'
        verbose_name_plural = 'Genres'

    def __str__(self):
        return self.name
class Movie(TimestampedModel):
    """
    Core movie entity.
    
    Sources:
    - External: Ingested from TheMovieDB (tmdb_id is not null)
    - Internal: User-created content (tmdb_id is null)
    
    """
    
    class Status(models.TextChoices):
        RUMORED = 'rumored', 'Rumored'
        PLANNED = 'planned', 'Planned'
        IN_PRODUCTION = 'in_production', 'In Production'
        POST_PRODUCTION = 'post_production', 'Post Production'
        RELEASED = 'released', 'Released'
        CANCELED = 'canceled', 'Canceled'
    class Source(models.TextChoices):
        EXTERNAL = 'external', 'External (TMDB)'
        INTERNAL = 'internal', 'Internal (User Created)'
        
    id=models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    
    tmdb_id = models.IntegerField(
        unique=True,
        null=True,
        blank=True,
        db_index=True,
        help_text="External ID from TheMovieDB"
    )
    
    source = models.CharField(
        max_length=20,
        choices=Source.choices,
        default=Source.INTERNAL,
        db_index=True
    )
    
    title = models.CharField(max_length=255, db_index=True)
    original_title = models.CharField(max_length=255, blank=True)
    overview = models.TextField(blank=True)
    tagline = models.CharField(max_length=500, blank=True)
    description = models.TextField(blank=True)
    release_date = models.DateField(null=True, blank=True, db_index=True)
    runtime = models.PositiveIntegerField(
        null=True,
        blank=True,
        help_text="Runtime in minutes"
    )
    
    popularity = models.DecimalField(
        max_digits=8,
        decimal_places=4,
        null=True,
        db_index=True,
        blank=True,
        validators=[MinValueValidator(Decimal('0.0'))],
        help_text="Popularity score"
    )
    
    vote_average = models.DecimalField(
        max_digits=3,
        decimal_places=1,
        default= Decimal('0.0'),
        null=True,
        blank=True,
        validators=[MinValueValidator(Decimal('0.0')), MaxValueValidator(Decimal('10.0'))],
    )
    
    vote_count = models.PositiveIntegerField(
        default=0,
    )
    
    poster_path = models.URLField(blank=True)
    backdrop_path = models.URLField(blank=True,null=True)
    
    status = models.CharField(
        max_length=20,
        choices=Status.choices,
        default=Status.RELEASED,
        db_index=True
    )
       
    genres = models.ManyToManyField(Genre, related_name='movies', blank=True)
    
    search_vector = SearchVectorField(null=True, blank=True)
    
    synced_to_mongo_at = models.DateTimeField(null=True, blank=True,help_text="Timestamp of last sync to MongoDB")
    
    is_active = models.BooleanField(default=True, db_index=True)
    
    class Meta:
        db_table = 'movies'
        ordering = ['-popularity', '-release_date']
        indexes = [
            models.Index(
                fields=['is_active', '-popularity'],
                name='movies_active_pop_idx'
            ),
            models.Index(
                fields=['-release_date', '-popularity'],
                name='movies_date_pop_idx'
            ),
            models.Index(
                fields=['source', '-created_at'],
                name='movies_source_created_idx'
            ),
            models.Index(
                fields=['status', '-release_date'],
                name='movies_status_date_idx'
            ),
            GinIndex(
                fields=['search_vector'],
                name='movies_search_idx'
            ),
        ]
        verbose_name = 'Movie'
        verbose_name_plural = 'Movies'

    def __str__(self):
        return f"{self.title} ({self.release_date.year if self.release_date else 'N/A'})"
    
    def save(self, *args, **kwargs):
    # Set source based on tmdb_id
        if self.tmdb_id is not None:
            self.source = self.Source.EXTERNAL
        else:
            self.source = self.Source.INTERNAL

        # ALWAYS save the object
        super().save(*args, **kwargs)

        # PostgreSQL-only full text search
        if connection.vendor == "postgresql" and self.title:
            Movie.objects.filter(pk=self.pk).update(
                search_vector=SearchVector(
                    'title',
                    'original_title',
                    'description',
                )
            )
    
    @property
    def is_external(self):
        """Check if movie is from external source."""
        return self.source == self.Source.EXTERNAL
    @property
    def is_internal(self):
        """Check if movie is user-created."""
        return self.source == self.Source.INTERNAL
    def mark_as_synced(self, timestamp):
        """Update the synced_to_mongo_at timestamp."""
        self.synced_to_mongo_at = timestamp.now()
        self.save(update_fields=['synced_to_mongo_at'])
    
class Review(TimestampedModel):
        """User review for a movie"""
    
        id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
        movie = models.ForeignKey(
          Movie,
          on_delete=models.CASCADE, 
          related_name='reviews')
    
        user= models.ForeignKey(
        'auth.User',
        on_delete=models.CASCADE,
        related_name='movie_reviews'
        )
    
        rating = models.DecimalField(
            max_digits=2,
            decimal_places=1,
            validators=[MinValueValidator(Decimal('0.0')), MaxValueValidator(Decimal('10.0'))],
        )
    
        title = models.CharField(max_length=255)
        content= models.TextField()
    
        class Meta:
         db_table = 'reviews'
         ordering = ['-created_at']
         unique_together = ('movie', 'user')
         verbose_name = 'Review'
         verbose_name_plural = 'Reviews'
         constraints = [
            models.UniqueConstraint(
                fields=['movie', 'user'],
                name='unique_movie_user_review'
            )
        ]
         indexes = [
            models.Index(
                fields=['movie', '-created_at'],
                name='reviews_movie_created_idx'
            ),
        ]
        def __str__(self):
            return f"{self.user.username}'s review of {self.movie.title}"
    
    
class IngestionLog(TimestampedModel):
       """Log of movie data ingestion from external sources."""
       class JobType(models.TextChoices):
              POPULAR_MOVIES = 'popular_movies', 'Popular Movies'
              TOP_RATED_MOVIES = 'top_rated_movies', 'Top Rated Movies'
              UPCOMING_MOVIES = 'upcoming_movies', 'Upcoming Movies'
              
       class Status(models.TextChoices):
              STARTED = 'started', 'Started'
              COMPLETED = 'completed', 'Completed'
              FAILED = 'failed', 'Failed'
              PARTIAL='partial', 'Partial'
              
       id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
       job_type = models.CharField(
              max_length=20,
              choices=JobType.choices,
              db_index=True
       )
       started_at = models.DateTimeField(auto_now_add=True)
       completed_at = models.DateTimeField(null=True, blank=True)
       status = models.CharField(
              max_length=20,
              choices=Status.choices,
              default=Status.STARTED,
              db_index=True
       )
       movies_fetched = models.PositiveIntegerField(default=0)
       movies_created = models.PositiveIntegerField(default=0)
       movies_updated = models.PositiveIntegerField(default=0)
       errors=models.JSONField(default=list, blank=True,help_text="List of errors encountered during ingestion")
       metadata=models.JSONField(default=dict, blank=True,help_text="Additional metadata about the ingestion job")
       class Meta:
              db_table = 'ingestion_logs'
              ordering = ['-started_at']
              verbose_name = 'Ingestion Log'
              verbose_name_plural = 'Ingestion Logs'
              indexes = [
                     models.Index(
                            fields=['job_type', '-started_at'],
                            name='ing_job_started_idx'
                     ),
                     models.Index(
                            fields=['status', '-started_at'],
                            name='ing_status_started_idx'
                     ),
              ]
       def __str__(self):
            return f"{self.get_job_type_display()} - {self.status} ({self.started_at})"
        
       @property
       def duration(self):
            """Calculate duration of the ingestion job in seconds."""
            if self.completed_at:
                return (self.completed_at - self.started_at).total_seconds()
            return None
        