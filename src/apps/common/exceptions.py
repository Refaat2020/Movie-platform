"""
Custom exception classes and global exception handler.

Provides:
- Conventional HTTP status codes
- Consistent error response format
- Detailed error messages for debugging
- Request ID tracking for support tickets
"""
from rest_framework.views import exception_handler
from rest_framework.exceptions import APIException
from rest_framework import status
from django.utils import timezone
import uuid


class BaseAPIException(APIException):
    """
    Base exception class for custom API errors.
    Allows adding extra context to the error response.
    """
    status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
    default_code = 'error'
    default_detail = 'An error occurred'

    def __init__(self, detail=None, code=None, **kwargs):
        self.extra = kwargs
        super().__init__(detail, code)


class NotFoundError(BaseAPIException):
    """Resource not found (404)."""
    status_code = status.HTTP_404_NOT_FOUND
    default_code = 'RESOURCE_NOT_FOUND'
    default_detail = 'The requested resource does not exist'


class ConflictError(BaseAPIException):
    """Resource conflict (409)."""
    status_code = status.HTTP_409_CONFLICT
    default_code = 'RESOURCE_CONFLICT'
    default_detail = 'The request conflicts with existing data'


class BadRequestError(BaseAPIException):
    """Bad request (400)."""
    status_code = status.HTTP_400_BAD_REQUEST
    default_code = 'BAD_REQUEST'
    default_detail = 'The request is invalid'


class UnauthorizedError(BaseAPIException):
    """Unauthorized (401)."""
    status_code = status.HTTP_401_UNAUTHORIZED
    default_code = 'UNAUTHORIZED'
    default_detail = 'Authentication credentials were not provided or are invalid'


class ForbiddenError(BaseAPIException):
    """Forbidden (403)."""
    status_code = status.HTTP_403_FORBIDDEN
    default_code = 'FORBIDDEN'
    default_detail = 'You do not have permission to perform this action'


class UnprocessableEntityError(BaseAPIException):
    """Unprocessable entity (422)."""
    status_code = status.HTTP_422_UNPROCESSABLE_ENTITY
    default_code = 'UNPROCESSABLE_ENTITY'
    default_detail = 'The request is well-formed but contains semantic errors'


class RateLimitExceededError(BaseAPIException):
    """Rate limit exceeded (429)."""
    status_code = status.HTTP_429_TOO_MANY_REQUESTS
    default_code = 'RATE_LIMIT_EXCEEDED'
    default_detail = 'Too many requests. Please try again later'


class ServiceUnavailableError(BaseAPIException):
    """Service unavailable (503)."""
    status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    default_code = 'SERVICE_UNAVAILABLE'
    default_detail = 'The service is temporarily unavailable. Please try again later'


class ExternalServiceError(BaseAPIException):
    """External service error (502)."""
    status_code = status.HTTP_502_BAD_GATEWAY
    default_code = 'EXTERNAL_SERVICE_ERROR'
    default_detail = 'An external service encountered an error'


def custom_exception_handler(exc, context):
    """
    Custom exception handler for DRF views.
    Formats error responses consistently and logs server errors.
    """
    response = exception_handler(exc, context)
    
    if response is not None:
        request_id = str(uuid.uuid4())
    
        request = context.get('request')
        if request and hasattr(request, 'id'):
            request_id = request.id
      
        error_code = getattr(exc, 'default_code', 'ERROR')
    
        if hasattr(exc, 'get_codes'):
            codes = exc.get_codes()
            if isinstance(codes, dict):
                error_code = 'VALIDATION_ERROR'
        
        custom_response_data = {
            'error': {
                'code': error_code,
                'message': str(exc.detail) if hasattr(exc, 'detail') else str(exc),
                'timestamp': timezone.now().isoformat(),
                'request_id': request_id,
            }
        }
   
        if hasattr(exc, 'detail') and isinstance(exc.detail, dict):
            custom_response_data['error']['details'] = exc.detail

        if isinstance(exc, BaseAPIException) and exc.extra:
            custom_response_data['error']['details'] = exc.extra

        if response.status_code >= 500:
            import logging
            logger = logging.getLogger(__name__)
            logger.error(
                f"Server error: {exc}",
                extra={
                    'request_id': request_id,
                    'status_code': response.status_code,
                    'exception': str(exc),
                    'request_path': request.path if request else None,
                }
            )
        
        response.data = custom_response_data
    
    return response




def raise_not_found(resource_type, resource_id=None, message=None):
    """
    Raise NotFoundError with consistent format.
    Example:
        raise_not_found('Movie', movie_id, 'Movie with given ID does not exist')
    """
    details = {'resource_type': resource_type}
    if resource_id:
        details['resource_id'] = str(resource_id)
    
    detail = message or f"{resource_type} not found"
    raise NotFoundError(detail=detail, **details)


def raise_conflict(message, **details):
    """
    Raise ConflictError with details.
    
    Example:
        raise_conflict('Review already exists', movie_id=movie_id, user_id=user_id)
    """
    raise ConflictError(detail=message, **details)


def raise_validation_error(message, **details):
    """
    Raise UnprocessableEntityError for semantic validation.
    
    Example:
        raise_validation_error('Rating must be provided with review content')
    """
    raise UnprocessableEntityError(detail=message, **details)
