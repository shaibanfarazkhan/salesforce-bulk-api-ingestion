"""
API Presentation Layer Package Initialization.
Exports api blueprint for application factory registration.
"""

from app.api.routes import api_bp

__all__ = ["api_bp"]
