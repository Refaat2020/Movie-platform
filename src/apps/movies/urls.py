"""
URL configuration for Movies app.
"""
from django.urls import path, include
from rest_framework.routers import DefaultRouter

from src.apps.movies.views import MovieViewSet, GenreViewSet, ReviewViewSet

router = DefaultRouter()
router.register(r'genres', GenreViewSet, basename='genre')
router.register(r'reviews', ReviewViewSet, basename='review')
router.register(r'', MovieViewSet, basename='movie')

app_name = 'movies'

urlpatterns = [
    path('', include(router.urls)),
]
