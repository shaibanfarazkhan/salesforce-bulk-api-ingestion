"""
Authentication DTO Serialization Schemas.
Validates credentials for OAuth token requests and serializes token responses.
"""

from marshmallow import Schema, fields, validate

class LoginRequestSchema(Schema):
    """
    Schema for validating login credential inputs.
    
    TODO [Phase 7]: Bind to OAuth 2.0 Web Server & Password flows.
    """
    username = fields.String(
        required=True,
        validate=validate.Length(min=3, max=255),
        metadata={"description": "Salesforce Integration Username / Email"}
    )
    password = fields.String(
        required=True,
        load_only=True,
        metadata={"description": "Salesforce User Password"}
    )
    security_token = fields.String(
        required=False,
        load_only=True,
        metadata={"description": "Salesforce Security Token"}
    )
    client_id = fields.String(
        required=False,
        metadata={"description": "Connected App Client ID"}
    )
    client_secret = fields.String(
        required=False,
        load_only=True,
        metadata={"description": "Connected App Client Secret"}
    )

class TokenResponseSchema(Schema):
    """
    Schema for serializing successful OAuth session tokens.
    """
    access_token = fields.String(required=True)
    instance_url = fields.String(required=True)
    token_type = fields.String(dump_default="Bearer")
    expires_in = fields.Integer(dump_default=7200)
