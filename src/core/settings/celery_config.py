"""
Celery settings to be included in Django settings.

Add to your base settings:
    from .celery_config import *
"""
import os

# Celery Broker Configuration
CELERY_BROKER_URL = os.getenv(
    'CELERY_BROKER_URL',
    'amqp://guest:guest@localhost:5672//'  # RabbitMQ
)

# Celery Result Backend (Redis)
CELERY_RESULT_BACKEND = os.getenv(
    'CELERY_RESULT_BACKEND',
    'redis://localhost:6379/1'
)

# Task serialization
CELERY_TASK_SERIALIZER = 'json'
CELERY_RESULT_SERIALIZER = 'json'
CELERY_ACCEPT_CONTENT = ['json']
CELERY_TIMEZONE = 'UTC'

# Task execution settings
CELERY_TASK_TRACK_STARTED = True  # Track when task starts
CELERY_TASK_TIME_LIMIT = 30 * 60  # Hard time limit: 30 minutes
CELERY_TASK_SOFT_TIME_LIMIT = 25 * 60  # Soft time limit: 25 minutes

# Result backend settings
CELERY_RESULT_EXPIRES = 60 * 60 * 24  # Results expire after 24 hours
CELERY_RESULT_PERSISTENT = True  # Persist results across restarts

# Worker settings
CELERY_WORKER_PREFETCH_MULTIPLIER = 4  # How many messages to prefetch
CELERY_WORKER_MAX_TASKS_PER_CHILD = 1000  # Restart worker after 1000 tasks
CELERY_WORKER_DISABLE_RATE_LIMITS = False

# Task routing
CELERY_TASK_ROUTES = {
    'apps.integrations.tmdb.tasks.*': {
        'queue': 'tmdb_ingestion',
        'routing_key': 'tmdb.ingestion',
    },
    'apps.integrations.mongodb.tasks.*': {
        'queue': 'mongodb_sync',
        'routing_key': 'mongodb.sync',
    },
}

# Task priority (lower number = higher priority)
CELERY_TASK_DEFAULT_PRIORITY = 5
CELERY_TASK_ACKS_LATE = True  # Acknowledge task after completion
CELERY_TASK_REJECT_ON_WORKER_LOST = True  # Requeue task if worker dies

# Retry policy defaults
CELERY_TASK_AUTORETRY_FOR = (Exception,)
CELERY_TASK_RETRY_BACKOFF = True
CELERY_TASK_RETRY_BACKOFF_MAX = 600  # Max 10 minutes between retries
CELERY_TASK_MAX_RETRIES = 5
CELERY_TASK_RETRY_JITTER = True  # Add randomness to retry delays

# Beat scheduler
CELERY_BEAT_SCHEDULER = 'django_celery_beat.schedulers:DatabaseScheduler'

# Logging
CELERY_WORKER_HIJACK_ROOT_LOGGER = False  # Don't override root logger
CELERY_WORKER_LOG_FORMAT = '[%(asctime)s: %(levelname)s/%(processName)s] %(message)s'
CELERY_WORKER_TASK_LOG_FORMAT = (
    '[%(asctime)s: %(levelname)s/%(processName)s] '
    '[%(task_name)s(%(task_id)s)] %(message)s'
)

# Monitoring (optional: requires Flower or similar)
CELERY_SEND_TASK_SENT_EVENT = True
CELERY_SEND_TASK_ERROR_EMAILS = True  # Email on task failure

# Security
CELERY_TASK_ALWAYS_EAGER = False  # Don't run tasks synchronously (except in tests)

# TMDB-specific settings
TMDB_API_KEY = os.getenv('TMDB_API_KEY', '')
TMDB_IMAGE_BASE_URL = 'https://image.tmdb.org/t/p'

# Configurable schedules (see celery.py for usage)
CELERY_POPULAR_MOVIES_SCHEDULE = os.getenv('CELERY_POPULAR_MOVIES_SCHEDULE', 'daily_2am')
CELERY_TRENDING_MOVIES_SCHEDULE = os.getenv('CELERY_TRENDING_MOVIES_SCHEDULE', 'every_6_hours')
CELERY_GENRE_SYNC_SCHEDULE = os.getenv('CELERY_GENRE_SYNC_SCHEDULE', 'weekly')
CELERY_UPCOMING_MOVIES_SCHEDULE = os.getenv('CELERY_UPCOMING_MOVIES_SCHEDULE', 'daily_3am')

# Pages to fetch per ingestion
TMDB_POPULAR_PAGES = int(os.getenv('TMDB_POPULAR_PAGES', '5'))
TMDB_UPCOMING_PAGES = int(os.getenv('TMDB_UPCOMING_PAGES', '3'))
TMDB_TRENDING_WINDOW = os.getenv('TMDB_TRENDING_WINDOW', 'day')  # 'day' or 'week'
TMDB_REGION = os.getenv('TMDB_REGION', None)  # e.g., 'US', 'GB'
