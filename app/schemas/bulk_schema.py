"""
Bulk API 2.0 & Ingestion DTO Schemas.
Validates job dispatch requests, SOQL parameters, and serializes job audit reports.
"""

from marshmallow import Schema, fields, validate

class BulkJobCreateSchema(Schema):
    """
    Schema for creating a new Salesforce Bulk API 2.0 query job.
    
    TODO [Phase 8]: Validate SOQL syntax and object compatibility.
    """
    object = fields.String(
        required=True,
        validate=validate.OneOf(["Account", "Contact", "Opportunity", "Lead"]),
        metadata={"description": "Target Salesforce SObject name"}
    )
    query = fields.String(
        required=True,
        validate=validate.Length(min=10),
        metadata={"description": "Valid SOQL query string"}
    )
    operation = fields.String(
        dump_default="query",
        load_default="query",
        validate=validate.Equal("query")
    )

class BulkJobResponseSchema(Schema):
    """
    Schema for serializing Bulk API 2.0 query job descriptors.
    """
    id = fields.String(required=True)
    state = fields.String(required=True)
    object = fields.String(required=True)
    operation = fields.String(required=True)
    records_processed = fields.Integer(dump_default=0)
    created_date = fields.String()

class IngestRequestSchema(Schema):
    """
    Schema for triggering an end-to-end data ingestion pipeline.
    
    TODO [Phase 10]: Add incremental CDC timestamp filters.
    """
    object = fields.String(
        required=True,
        validate=validate.OneOf(["Account", "Contact", "Opportunity"])
    )
    soql = fields.String(required=True)
    batch_size = fields.Integer(
        load_default=10000,
        validate=validate.Range(min=500, max=50000)
    )
    incremental_sync = fields.Boolean(load_default=False)
    since_timestamp = fields.DateTime(required=False)
