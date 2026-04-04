from django.urls import path
from drf_spectacular.utils import extend_schema
from rest_framework_simplejwt.views import TokenRefreshView

from . import views

# Fix TokenRefreshView tag so it appears under Auth in Swagger
TokenRefreshView = extend_schema(
    tags=["Auth"],
    summary="Refresh access token",
    description="Send a valid refresh token to get a new access token.",
)(TokenRefreshView)

app_name = "accounts"

urlpatterns = [
    path("register/",        views.RegisterView.as_view(),       name="register"),
    path("login/",           views.LoginView.as_view(),          name="login"),
    path("logout/",          views.LogoutView.as_view(),         name="logout"),
    path("token/refresh/",   TokenRefreshView.as_view(),
         name="token-refresh"),
    path("profile/",         views.ProfileView.as_view(),        name="profile"),
    path("change-password/", views.ChangePasswordView.as_view(),
         name="change-password"),
]
