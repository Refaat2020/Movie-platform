"""
Custom pagination classes for consistent API responses.

Provides standard pagination with metadata about total count, pages, etc.
"""
from collections import OrderedDict

from rest_framework.pagination import PageNumberPagination
from rest_framework.response import Response


class StandardResultsSetPagination(PageNumberPagination):
    """
    Standard pagination for list endpoints.
    
    Configuration:
    - Default page size: 20 items
    - Max page size: 100 items
    - Allows client to customize via ?page_size= parameter
    
    Response format:
    {
        "count": 150,
        "page": 2,
        "page_size": 20,
        "total_pages": 8,
        "next": "http://api.example.com/movies/?page=3",
        "previous": "http://api.example.com/movies/?page=1",
        "results": [...]
    }
    """
    
    page_size = 20
    page_size_query_param = 'page_size'
    max_page_size = 100
    page_query_param = 'page'

    def get_paginated_response(self, data):
        """
        Return custom paginated response with additional metadata.
        """
        return Response(OrderedDict([
            ('count', self.page.paginator.count),
            ('page', self.page.number),
            ('page_size', self.page.paginator.per_page),
            ('total_pages', self.page.paginator.num_pages),
            ('next', self.get_next_link()),
            ('previous', self.get_previous_link()),
            ('results', data)
        ]))

    def get_paginated_response_schema(self, schema):
        """
        Schema for OpenAPI documentation.
        
        This generates proper API docs in Swagger/ReDoc.
        """
        return {
            'type': 'object',
            'properties': {
                'count': {
                    'type': 'integer',
                    'example': 150,
                    'description': 'Total number of items'
                },
                'page': {
                    'type': 'integer',
                    'example': 2,
                    'description': 'Current page number'
                },
                'page_size': {
                    'type': 'integer',
                    'example': 20,
                    'description': 'Number of items per page'
                },
                'total_pages': {
                    'type': 'integer',
                    'example': 8,
                    'description': 'Total number of pages'
                },
                'next': {
                    'type': 'string',
                    'nullable': True,
                    'format': 'uri',
                    'example': 'http://api.example.com/movies/?page=3',
                    'description': 'URL to next page'
                },
                'previous': {
                    'type': 'string',
                    'nullable': True,
                    'format': 'uri',
                    'example': 'http://api.example.com/movies/?page=1',
                    'description': 'URL to previous page'
                },
                'results': schema,
            },
        }


class LargeResultsSetPagination(PageNumberPagination):
    """
    Pagination for endpoints with large datasets.
    Used for reporting.
    """
    
    page_size = 50
    page_size_query_param = 'page_size'
    max_page_size = 200
    page_query_param = 'page'

    def get_paginated_response(self, data):
        return Response(OrderedDict([
            ('count', self.page.paginator.count),
            ('page', self.page.number),
            ('page_size', self.page.paginator.per_page),
            ('total_pages', self.page.paginator.num_pages),
            ('next', self.get_next_link()),
            ('previous', self.get_previous_link()),
            ('results', data)
        ]))


class SmallResultsSetPagination(PageNumberPagination):
    """
    Pagination for endpoints with small datasets.
    """
    
    page_size = 10
    page_size_query_param = 'page_size'
    max_page_size = 50
    page_query_param = 'page'

    def get_paginated_response(self, data):
        return Response(OrderedDict([
            ('count', self.page.paginator.count),
            ('page', self.page.number),
            ('page_size', self.page.paginator.per_page),
            ('total_pages', self.page.paginator.num_pages),
            ('next', self.get_next_link()),
            ('previous', self.get_previous_link()),
            ('results', data)
        ]))
