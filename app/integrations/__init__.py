"""
Integrations Package Initialization.
Exports SalesforceClient and SalesforceOAuth authentication manager.
"""

from app.integrations.auth import SalesforceOAuth
from app.integrations.salesforce_client import SalesforceClient

__all__ = [
    "SalesforceOAuth",
    "SalesforceClient"
]
