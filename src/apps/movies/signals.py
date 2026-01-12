"""
Django signals for triggering MongoDB sync.

Design Decisions:
-----------------
We use signals instead of adding sync logic to models/views because:
1. Separation of concerns: Models don't need to know about MongoDB
2. Testability: Can disable signals in tests
3. Flexibility: Can add/remove sync without changing core code
4. Async by default: Signals trigger Celery tasks, not blocking sync

Signal Flow:
-----------
Model save/delete → Signal → Celery task → MongoDB sync

Why Celery tasks instead of direct sync?
- Don't block API responses
- Automatic retry on failure
- Can batch operations
- Monitoring via Flower
"""
import logging
from django.db.models.signals import post_save, post_delete, m2m_changed
from django.dispatch import receiver
from django.conf import settings

from src.apps.movies.models import Movie, Review

logger = logging.getLogger(__name__)


@receiver(post_save, sender=Movie)
def sync_movie_on_save(sender, instance, created, **kwargs):
    """
    Trigger MongoDB sync when a movie is created or updated.
    
    Args:
        instance: Movie model instance
        created: True if newly created
    
    Behavior:
    - Async by default (queues Celery task)
    - Can be disabled via settings.ENABLE_MONGODB_SYNC
    - Skip sync if raw=True (e.g., fixtures, migrations)
    """
    # Skip if MongoDB sync is disabled
    if not getattr(settings, 'ENABLE_MONGODB_SYNC', True):
        return
    
    # Skip if raw save (fixtures, migrations)
    if kwargs.get('raw', False):
        return
    
    # Import here to avoid circular imports
    from src.apps.integrations.mongodb.tasks import sync_movie_to_mongodb
    
    # Queue async task
    sync_movie_to_mongodb.delay(str(instance.id))
    
    action = "created" if created else "updated"
    logger.debug(f"Queued MongoDB sync for {action} movie: {instance.title}")


@receiver(post_delete, sender=Movie)
def sync_movie_on_delete(sender, instance, **kwargs):
    """
    Trigger MongoDB deletion when a movie is deleted.
    
    Note: This handles hard deletes. For soft deletes (is_active=False),
    the post_save signal will handle updating MongoDB.
    
    Args:
        instance: Movie model instance being deleted
    """
    # Skip if MongoDB sync is disabled
    if not getattr(settings, 'ENABLE_MONGODB_SYNC', True):
        return
    
    from src.apps.integrations.mongodb.tasks import delete_movie_from_mongodb
    
    # Queue async deletion
    delete_movie_from_mongodb.delay(str(instance.id))
    
    logger.debug(f"Queued MongoDB deletion for movie: {instance.title}")


@receiver(m2m_changed, sender=Movie.genres.through)
@receiver(m2m_changed, sender=Movie.production_companies.through)
def sync_movie_on_m2m_change(sender, instance, action, **kwargs):
    """
    Trigger MongoDB sync when movie relationships change.
    
    This handles many-to-many relationships:
    - Genres added/removed
    - Production companies added/removed
    
    Why needed?
    - Genres and companies are denormalized in MongoDB
    - Changes to these relationships need to be synced
    
    Args:
        instance: Movie model instance
        action: M2M action (post_add, post_remove, post_clear)
    """
    # Only sync after M2M changes are complete
    if action not in ['post_add', 'post_remove', 'post_clear']:
        return
    
    # Skip if MongoDB sync is disabled
    if not getattr(settings, 'ENABLE_MONGODB_SYNC', True):
        return
    
    from src.apps.integrations.mongodb.tasks import sync_movie_to_mongodb
    
    # Queue async sync
    sync_movie_to_mongodb.delay(str(instance.id))
    
    logger.debug(f"Queued MongoDB sync for movie M2M change: {instance.title}")


@receiver(post_save, sender=Review)
def sync_review_on_save(sender, instance, created, **kwargs):
    """
    Trigger MongoDB sync when a review is created or updated.
    
    This triggers two syncs:
    1. Sync the review document itself
    2. Sync the parent movie (to update review_stats)
    
    Args:
        instance: Review model instance
        created: True if newly created
    """
    # Skip if MongoDB sync is disabled
    if not getattr(settings, 'ENABLE_MONGODB_SYNC', True):
        return
    
    # Skip if raw save
    if kwargs.get('raw', False):
        return
    
    from src.apps.integrations.mongodb.tasks import sync_review_to_mongodb
    
    # Queue async task (this will also sync parent movie)
    sync_review_to_mongodb.delay(str(instance.id))
    
    action = "created" if created else "updated"
    logger.debug(f"Queued MongoDB sync for {action} review: {instance.id}")


@receiver(post_delete, sender=Review)
def sync_review_on_delete(sender, instance, **kwargs):
    """
    Trigger MongoDB deletion when a review is deleted.
    
    Also triggers re-sync of parent movie to update review_stats.
    
    Args:
        instance: Review model instance being deleted
    """
    # Skip if MongoDB sync is disabled
    if not getattr(settings, 'ENABLE_MONGODB_SYNC', True):
        return
    
    from src.apps.integrations.mongodb.tasks import delete_review_from_mongodb
    
    # Queue async deletion (will also re-sync parent movie)
    delete_review_from_mongodb.delay(
        str(instance.id),
        str(instance.movie.id)
    )
    
    logger.debug(f"Queued MongoDB deletion for review: {instance.id}")


# Optional: Connect signals only if MongoDB sync is enabled
def connect_mongodb_signals():
    """
    Connect MongoDB sync signals.
    
    Call this in apps.py ready() method to enable signals.
    
    Example:
        # apps/movies/apps.py
        def ready(self):
            import apps.movies.signals
            apps.movies.signals.connect_mongodb_signals()
    """
    # Signals are connected via @receiver decorators
    # This function is just for documentation
    pass


def disconnect_mongodb_signals():
    """
    Disconnect MongoDB sync signals.
    
    Useful for:
    - Testing (to disable signals)
    - Data migrations (to avoid triggering syncs)
    - Bulk operations (sync manually after completion)
    
    Example:
        # In tests
        from apps.movies import signals
        signals.disconnect_mongodb_signals()
        
        # Do test operations
        
        signals.connect_mongodb_signals()
    """
    post_save.disconnect(sync_movie_on_save, sender=Movie)
    post_delete.disconnect(sync_movie_on_delete, sender=Movie)
    m2m_changed.disconnect(sync_movie_on_m2m_change, sender=Movie.genres.through)
    m2m_changed.disconnect(sync_movie_on_m2m_change, sender=Movie.production_companies.through)
    post_save.disconnect(sync_review_on_save, sender=Review)
    post_delete.disconnect(sync_review_on_delete, sender=Review)
