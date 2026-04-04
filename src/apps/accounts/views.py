from drf_spectacular.utils import (
    OpenApiExample,
    OpenApiResponse,
    extend_schema,
    extend_schema_view,
)
from drf_spectacular.openapi import AutoSchema
from rest_framework import generics, permissions, status
from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework_simplejwt.tokens import RefreshToken
from rest_framework_simplejwt.views import TokenObtainPairView

from .serializers import (
    ChangePasswordSerializer,
    CustomTokenObtainPairSerializer,
    RegisterSerializer,
    UpdateProfileSerializer,
    UserSerializer,
)


# ─────────────────────────────────────────
# Register
# ─────────────────────────────────────────
@extend_schema(
    tags=["Auth"],
    summary="Register a new user",
    description="Creates a new account and returns JWT access + refresh tokens immediately.",
    responses={
        201: OpenApiResponse(description="Account created — includes tokens and user data"),
        400: OpenApiResponse(description="Validation error (duplicate email, password mismatch, etc.)"),
    },
    examples=[
        OpenApiExample(
            "Register example",
            value={
                "username": "refaat",
                "email": "refaat@example.com",
                "first_name": "Refaat",
                "last_name": "",
                "password": "StrongPass123!",
                "password_confirm": "StrongPass123!",
            },
            request_only=True,
        )
    ],
)
class RegisterView(generics.CreateAPIView):
    from django.contrib.auth import get_user_model
    queryset = get_user_model().objects.all()
    serializer_class = RegisterSerializer
    permission_classes = [permissions.AllowAny]

    def create(self, request, *args, **kwargs):
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        user = serializer.save()

        refresh = RefreshToken.for_user(user)
        refresh["username"] = user.username
        refresh["email"] = user.email

        return Response(
            {
                "message": "Account created successfully.",
                "user": UserSerializer(user).data,
                "tokens": {
                    "access": str(refresh.access_token),
                    "refresh": str(refresh),
                },
            },
            status=status.HTTP_201_CREATED,
        )


# ─────────────────────────────────────────
# Login
# ─────────────────────────────────────────
@extend_schema(
    tags=["Auth"],
    summary="Login — get JWT tokens",
    description="Authenticate with email + password. Returns access token (60 min) and refresh token (7 days).",
    examples=[
        OpenApiExample(
            "Login example",
            value={"email": "refaat@example.com",
                   "password": "StrongPass123!"},
            request_only=True,
        )
    ],
)
class LoginView(TokenObtainPairView):
    serializer_class = CustomTokenObtainPairSerializer
    permission_classes = [permissions.AllowAny]


# ─────────────────────────────────────────
# Logout
# ─────────────────────────────────────────
@extend_schema(
    tags=["Auth"],
    summary="Logout — blacklist refresh token",
    description="Invalidates the provided refresh token. Send the refresh token in the request body.",
    request={"application/json": {"type": "object",
                                  "properties": {"refresh": {"type": "string"}}, "required": ["refresh"]}},
    responses={
        200: OpenApiResponse(description="Logged out successfully"),
        400: OpenApiResponse(description="Missing or invalid refresh token"),
    },
)
class LogoutView(APIView):
    permission_classes = [permissions.IsAuthenticated]

    def post(self, request):
        try:
            refresh_token = request.data.get("refresh")
            if not refresh_token:
                return Response({"detail": "Refresh token is required."}, status=status.HTTP_400_BAD_REQUEST)
            token = RefreshToken(refresh_token)
            token.blacklist()
            return Response({"message": "Logged out successfully."}, status=status.HTTP_200_OK)
        except Exception:
            return Response({"detail": "Invalid or expired token."}, status=status.HTTP_400_BAD_REQUEST)


# ─────────────────────────────────────────
# Profile
# ─────────────────────────────────────────
@extend_schema_view(
    get=extend_schema(
        tags=["Profile"],
        summary="Get my profile",
        description="Returns the currently authenticated user's data.",
    ),
    patch=extend_schema(
        tags=["Profile"],
        summary="Update my profile",
        description="Update username, first/last name, bio, or avatar.",
    ),
    put=extend_schema(exclude=True),  # hide PUT, only show PATCH
)
class ProfileView(generics.RetrieveUpdateAPIView):
    permission_classes = [permissions.IsAuthenticated]

    def get_serializer_class(self):
        if self.request.method in ("PUT", "PATCH"):
            return UpdateProfileSerializer
        return UserSerializer

    def get_object(self):
        return self.request.user


# ─────────────────────────────────────────
# Change Password
# ─────────────────────────────────────────
@extend_schema(
    tags=["Profile"],
    summary="Change password",
    description="Requires old password + new password (confirmed).",
    responses={
        200: OpenApiResponse(description="Password changed successfully"),
        400: OpenApiResponse(description="Old password incorrect or passwords don't match"),
    },
)
class ChangePasswordView(APIView):
    permission_classes = [permissions.IsAuthenticated]

    def post(self, request):
        serializer = ChangePasswordSerializer(
            data=request.data, context={"request": request})
        serializer.is_valid(raise_exception=True)
        serializer.save()
        return Response({"message": "Password changed successfully."}, status=status.HTTP_200_OK)
