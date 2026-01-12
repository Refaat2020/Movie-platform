"""
Custom exceptions for TMDB integration.

Provides specific exception types for different failure scenarios.
"""


class TMDBAPIError(Exception):
    """Base exception for TMDB API errors."""
    pass


class TMDBRateLimitError(TMDBAPIError):
    """
    Raised when TMDB rate limit is exceeded.
    
    Attributes:
        retry_after: Seconds to wait before retrying
    """
    
    def __init__(self, message: str, retry_after: int = 10):
        super().__init__(message)
        self.retry_after = retry_after


class TMDBAuthError(TMDBAPIError):
    """Raised on authentication/authorization errors."""
    pass


class TMDBNotFoundError(TMDBAPIError):
    """Raised when requested resource is not found."""
    pass


class TMDBTimeoutError(TMDBAPIError):
    """Raised on request timeout."""
    pass


class TMDBConnectionError(TMDBAPIError):
    """Raised on connection errors."""
    pass
