"""
Celery configuration for movie platform.

Configures:
- RabbitMQ as message broker
- Redis as result backend
- Celery Beat for periodic tasks
- Task routing and priority
- Retry policies
"""
import os
from celery import Celery
from celery.schedules import crontab

# Set default Django settings module
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'core.settings.development')

# Create Celery app
app = Celery('core')

# Load config from Django settings with CELERY_ prefix
app.config_from_object('django.conf:settings', namespace='CELERY')

# Auto-discover tasks from installed apps
app.autodiscover_tasks([
    "src.apps.movies",
    "src.apps.integrations.tmdb",
])


# Celery Beat Schedule Configuration
@app.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    """
    Configure periodic tasks via Celery Beat.
    
    Schedule is configurable via environment variables:
    - CELERY_POPULAR_MOVIES_SCHEDULE: Cron expression (default: daily at 2 AM)
    - CELERY_TRENDING_MOVIES_SCHEDULE: Cron expression (default: every 6 hours)
    - CELERY_GENRE_SYNC_SCHEDULE: Cron expression (default: weekly on Sunday)
    """
    
    # Popular movies ingestion
    popular_schedule = os.getenv(
        'CELERY_POPULAR_MOVIES_SCHEDULE',
        'daily_2am'  # Default
    )
    
    if popular_schedule == 'hourly':
        schedule = crontab(minute=0)  # Every hour
    elif popular_schedule == 'daily_2am':
        schedule = crontab(hour=2, minute=0)  # Daily at 2 AM
    elif popular_schedule == 'twice_daily':
        schedule = crontab(hour='2,14', minute=0)  # 2 AM and 2 PM
    else:
        # Parse as cron expression
        schedule = crontab(popular_schedule)
    
    sender.add_periodic_task(
        schedule,
        popular_movies_periodic_task.s(),
        name='Ingest popular movies from TMDB',
    )
    
    # Trending movies ingestion
    trending_schedule = os.getenv(
        'CELERY_TRENDING_MOVIES_SCHEDULE',
        'every_6_hours'
    )
    
    if trending_schedule == 'every_6_hours':
        schedule = crontab(hour='*/6', minute=0)
    elif trending_schedule == 'hourly':
        schedule = crontab(minute=0)
    else:
        schedule = crontab(trending_schedule)
    
    sender.add_periodic_task(
        schedule,
        trending_movies_periodic_task.s(),
        name='Ingest trending movies from TMDB',
    )
    
    # Genre sync (less frequent)
    genre_schedule = os.getenv(
        'CELERY_GENRE_SYNC_SCHEDULE',
        'weekly'
    )
    
    if genre_schedule == 'weekly':
        schedule = crontab(day_of_week=0, hour=1, minute=0)  # Sunday 1 AM
    elif genre_schedule == 'monthly':
        schedule = crontab(day_of_month=1, hour=1, minute=0)  # 1st of month
    else:
        schedule = crontab(genre_schedule)
    
    sender.add_periodic_task(
        schedule,
        genre_sync_periodic_task.s(),
        name='Sync genres from TMDB',
    )
    
    # Upcoming movies ingestion
    upcoming_schedule = os.getenv(
        'CELERY_UPCOMING_MOVIES_SCHEDULE',
        'daily_3am'
    )
    
    if upcoming_schedule == 'daily_3am':
        schedule = crontab(hour=3, minute=0)
    else:
        schedule = crontab(upcoming_schedule)
    
    sender.add_periodic_task(
        schedule,
        upcoming_movies_periodic_task.s(),
        name='Ingest upcoming movies from TMDB',
    )


@app.task(bind=True)
def popular_movies_periodic_task(self):
    """
    Wrapper task for popular movies ingestion.
    
    Allows customizing parameters via environment variables.
    """
    from apps.integrations.tmdb.tasks import ingest_popular_movies
    
    pages = int(os.getenv('TMDB_POPULAR_PAGES', '5'))
    region = os.getenv('TMDB_REGION', None)
    
    return ingest_popular_movies.delay(pages=pages, region=region)


@app.task(bind=True)
def trending_movies_periodic_task(self):
    """Wrapper task for trending movies ingestion."""
    from apps.integrations.tmdb.tasks import ingest_trending_movies
    
    time_window = os.getenv('TMDB_TRENDING_WINDOW', 'day')
    return ingest_trending_movies.delay(time_window=time_window)


@app.task(bind=True)
def genre_sync_periodic_task(self):
    """Wrapper task for genre sync."""
    from apps.integrations.tmdb.tasks import refresh_genres
    
    return refresh_genres.delay()


@app.task(bind=True)
def upcoming_movies_periodic_task(self):
    """Wrapper task for upcoming movies ingestion."""
    from apps.integrations.tmdb.tasks import ingest_upcoming_movies
    
    pages = int(os.getenv('TMDB_UPCOMING_PAGES', '3'))
    return ingest_upcoming_movies.delay(pages=pages)


@app.task(bind=True)
def debug_task(self):
    """Debug task for testing Celery setup."""
    print(f'Request: {self.request!r}')
