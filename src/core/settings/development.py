# pylint: disable=relative-beyond-top-level, wildcard-import

from .base import  *  # 
from pathlib import Path
from dotenv import load_dotenv

DEBUG = True

load_dotenv()
TMDB_API_KEY = os.getenv("TMDB_API_KEY")
MONGODB_URI = os.getenv("MONGODB_URI")
ALLOWED_HOSTS = ["localhost", "127.0.0.1"]

BASE_DIR = Path(__file__).resolve().parent.parent.parent
# =========================
# Database (Local)
# =========================
DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": BASE_DIR / "db.sqlite3",
    }
}

# =========================
# Dev tools
# =========================
INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "django.contrib.postgres",
    "rest_framework",
    "drf_spectacular",
    "django_filters",
    # Local apps
    "src.apps.movies",
    "src.apps.integrations",
]

EMAIL_BACKEND = "django.core.mail.backends.console.EmailBackend"
