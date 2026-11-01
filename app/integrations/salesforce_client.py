"""
Salesforce REST & Bulk API 2.0 Integration Client.
Provides high-throughput, resilient, authenticated HTTP communications to
Salesforce Bulk API 2.0 endpoints with automatic 401 token refresh retry,
Governor Limit tracking, connection pooling, and request telemetry.
"""

import time
import re
import uuid
import logging
from typing import Dict, Any, Optional, Tuple, Iterator
from urllib.parse import urljoin
import requests
from flask import g, has_app_context, has_request_context

from app.integrations.auth import SalesforceOAuth
from app.utils.constants import HEADER_SF_LIMIT_INFO, HEADER_CORRELATION_ID, HEADER_LOCATOR
from app.utils.exceptions import (
    SalesforceAPIError,
    RateLimitExceededError,
    BulkJobException,
    InvalidSOQLError,
    ResourceNotFoundError,
    DownloadException
)
from app.utils.telemetry import metrics_collector

logger = logging.getLogger(__name__)

class SalesforceClient:
    """
    Enterprise client for Salesforce REST & Bulk API 2.0 Query services.
    """

    def __init__(
        self,
        oauth_manager: Optional[SalesforceOAuth] = None,
        api_version: str = "v62.0",
        timeout_seconds: int = 60
    ) -> None:
        self.oauth_manager = oauth_manager or SalesforceOAuth()
        self.api_version = api_version
        self.timeout_seconds = timeout_seconds
        self.last_api_usage: Optional[str] = None

    @property
    def instance_url(self) -> str:
        """Retrieve verified instance URL from OAuth manager."""
        return self.oauth_manager.get_instance_url()

    def get_headers(self, additional_headers: Optional[Dict[str, str]] = None) -> Dict[str, str]:
        """
        Construct standard authenticated headers with Bearer token, Correlation ID, and Request ID.
        """
        correlation_id = "SYSTEM"
        try:
            if has_app_context() and has_request_context() and hasattr(g, "correlation_id"):
                correlation_id = g.correlation_id
        except Exception:
            pass

        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json; charset=UTF-8",
            HEADER_CORRELATION_ID: correlation_id,
            "X-Request-ID": str(uuid.uuid4())
        }
        headers.update(self.oauth_manager.get_authorization_header())
        if additional_headers:
            headers.update(additional_headers)
        return headers

    def request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Any] = None,
        json: Optional[Any] = None,
        headers: Optional[Dict[str, str]] = None,
        stream: bool = False,
        retry_on_401: bool = True
    ) -> requests.Response:
        """
        Execute an authenticated HTTP request against Salesforce REST endpoints.
        Automatically intercepts 401 Unauthorized, forces token refresh, and re-dispatches once.
        """
        base_url = self.instance_url
        full_url = urljoin(f"{base_url}/", path.lstrip("/"))
        auth_headers = self.get_headers(headers)

        start_time = time.time()
        logger.debug(f"Salesforce API Request: {method.upper()} {full_url}")

        try:
            response = self.oauth_manager.session.request(
                method=method.upper(),
                url=full_url,
                params=params,
                data=data,
                json=json,
                headers=auth_headers,
                timeout=self.timeout_seconds,
                stream=stream
            )
        except requests.exceptions.Timeout as exc:
            logger.error(f"Request timeout to Salesforce endpoint {full_url}: {str(exc)}")
            raise SalesforceAPIError(f"Request timed out connecting to Salesforce: {str(exc)}", status_code=504) from exc
        except requests.exceptions.RequestException as exc:
            logger.error(f"HTTP error communicating with Salesforce: {str(exc)}")
            raise SalesforceAPIError(f"HTTP request to Salesforce failed: {str(exc)}") from exc

        # Telemetry & Governor Limit Tracking
        latency_ms = (time.time() - start_time) * 1000.0
        metrics_collector.record_api_latency(latency_ms)

        if HEADER_SF_LIMIT_INFO in response.headers:
            self.last_api_usage = response.headers[HEADER_SF_LIMIT_INFO]
            metrics_collector.set_api_usage(self.last_api_usage)
            logger.debug(f"Salesforce Governor Limits: {self.last_api_usage}")

        # Automatic Token Refresh on 401 Session Expiration
        if response.status_code == 401 and retry_on_401:
            logger.warning("Salesforce access token expired (HTTP 401). Forcing token refresh...")
            self.oauth_manager.authenticate(force_refresh=True)
            return self.request(
                method=method,
                path=path,
                params=params,
                data=data,
                json=json,
                headers=headers,
                stream=stream,
                retry_on_401=False
            )

        if response.status_code == 429:
            raise RateLimitExceededError(
                message="Salesforce API daily quota limit exceeded (HTTP 429)",
                details={"api_usage": self.last_api_usage}
            )

        return response

    # ==========================================================================
    # HTTP Method Wrappers for Salesforce REST APIs
    # ==========================================================================
    def get(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        stream: bool = False,
        **kwargs
    ) -> requests.Response:
        """Execute an authenticated HTTP GET request against Salesforce."""
        return self.request(method="GET", path=path, params=params, headers=headers, stream=stream, **kwargs)

    def post(
        self,
        path: str,
        data: Optional[Any] = None,
        json: Optional[Any] = None,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        **kwargs
    ) -> requests.Response:
        """Execute an authenticated HTTP POST request against Salesforce."""
        return self.request(method="POST", path=path, data=data, json=json, params=params, headers=headers, **kwargs)

    def patch(
        self,
        path: str,
        data: Optional[Any] = None,
        json: Optional[Any] = None,
        headers: Optional[Dict[str, str]] = None,
        **kwargs
    ) -> requests.Response:
        """Execute an authenticated HTTP PATCH request against Salesforce."""
        return self.request(method="PATCH", path=path, data=data, json=json, headers=headers, **kwargs)

    def delete(
        self,
        path: str,
        headers: Optional[Dict[str, str]] = None,
        **kwargs
    ) -> requests.Response:
        """Execute an authenticated HTTP DELETE request against Salesforce."""
        return self.request(method="DELETE", path=path, headers=headers, **kwargs)

    # ==========================================================================
    # Bulk API 2.0 Query Job Lifecycle Methods
    # ==========================================================================
    def create_query_job(self, soql_query: str, target_object: Optional[str] = "Account") -> Dict[str, Any]:
        """
        Submit a new Bulk API 2.0 query job.
        
        Endpoint: POST /services/data/vXX.X/jobs/query
        """
        if not soql_query or not isinstance(soql_query, str):
            raise InvalidSOQLError("SOQL query string cannot be empty or null.")

        soql = soql_query.strip()
        if not soql.upper().startswith("SELECT") or " FROM " not in soql.upper():
            raise InvalidSOQLError(
                f"Invalid SOQL query expression: '{soql}'. Must be a valid SELECT ... FROM statement."
            )

        if not target_object:
            match = re.search(r"\bFROM\s+([a-zA-Z0-9_]+)", soql, re.IGNORECASE)
            target_object = match.group(1) if match else "Account"

        path = f"/services/data/{self.api_version}/jobs/query"
        payload = {
            "operation": "query",
            "query": soql,
            "contentType": "CSV",
            "columnDelimiter": "COMMA",
            "lineEnding": "LF"
        }

        logger.info(f"Submitting Bulk API 2.0 query job for entity '{target_object}'...")
        response = self.request("POST", path, json=payload)

        if response.status_code not in (200, 201):
            raise BulkJobException(
                message=f"Failed to create Bulk API 2.0 query job: {response.text}",
                status_code=response.status_code,
                details={"response": response.text}
            )

        job_data = response.json()
        logger.info(
            f"Bulk API 2.0 query job created: id={job_data.get('id')}, "
            f"state={job_data.get('state')}, object={job_data.get('object', target_object)}"
        )
        return job_data

    def get_job_status(self, sf_job_id: str) -> Dict[str, Any]:
        """
        Fetch real-time status of a Bulk API 2.0 query job.
        
        Endpoint: GET /services/data/vXX.X/jobs/query/{jobId}
        """
        path = f"/services/data/{self.api_version}/jobs/query/{sf_job_id}"
        response = self.request("GET", path)

        if response.status_code != 200:
            raise BulkJobException(
                message=f"Failed to retrieve Bulk API job status for {sf_job_id}: {response.text}",
                status_code=response.status_code
            )

        return response.json()

    def abort_query_job(self, sf_job_id: str) -> Dict[str, Any]:
        """
        Abort an active Bulk API 2.0 query job.
        
        Endpoint: PATCH /services/data/vXX.X/jobs/query/{jobId} with {"state": "Aborted"}
        """
        path = f"/services/data/{self.api_version}/jobs/query/{sf_job_id}"
        payload = {"state": "Aborted"}

        logger.warning(f"Aborting Bulk API 2.0 query job: {sf_job_id}")
        response = self.request("PATCH", path, json=payload)

        if response.status_code != 200:
            raise BulkJobException(
                message=f"Failed to abort Bulk API query job {sf_job_id}: {response.text}",
                status_code=response.status_code
            )

        return response.json()

    def delete_query_job(self, sf_job_id: str) -> Dict[str, Any]:
        """
        Delete a Bulk API 2.0 query job on Salesforce (Cleanup).
        
        Endpoint: DELETE /services/data/vXX.X/jobs/query/{jobId}
        Salesforce returns HTTP 204 No Content upon successful deletion.
        """
        path = f"/services/data/{self.api_version}/jobs/query/{sf_job_id}"
        logger.info(f"Deleting Bulk API 2.0 query job for cleanup: {sf_job_id}")
        response = self.request("DELETE", path)

        if response.status_code in (200, 204):
            logger.info(f"Successfully deleted Bulk API query job {sf_job_id}")
            return {
                "id": sf_job_id,
                "deleted": True,
                "status": "DELETED",
                "message": f"Job {sf_job_id} deleted successfully from Salesforce."
            }
        elif response.status_code == 404:
            logger.warning(f"Bulk query job {sf_job_id} not found during cleanup.")
            raise ResourceNotFoundError(
                message=f"Bulk API query job {sf_job_id} not found on Salesforce.",
                status_code=404
            )
        else:
            raise BulkJobException(
                message=f"Failed to delete Bulk API query job {sf_job_id}: {response.text}",
                status_code=response.status_code,
                details={"response": response.text}
            )

    def get_job_results_stream(
        self,
        sf_job_id: str,
        locator: Optional[str] = None,
        max_records: int = 10000
    ) -> Tuple[requests.Response, Optional[str]]:
        """
        Initiate a chunked streaming download of Bulk API query results.
        
        Endpoint: GET /services/data/vXX.X/jobs/query/{jobId}/results
        Header: Sforce-Locator
        
        Returns:
            Tuple[requests.Response, Optional[str]]: (streaming_response, next_locator)
        """
        path = f"/services/data/{self.api_version}/jobs/query/{sf_job_id}/results"
        params: Dict[str, Any] = {"maxRecords": max_records}
        if locator and locator.lower() not in ("null", "none"):
            params["locator"] = locator

        headers = {"Accept": "text/csv"}

        response = self.request(
            method="GET",
            path=path,
            params=params,
            headers=headers,
            stream=True
        )

        if response.status_code != 200:
            raise DownloadException(
                message=f"Failed to stream Bulk API results for {sf_job_id}: {response.text}",
                status_code=response.status_code
            )

        # Extract pagination cursor from response header
        next_locator = response.headers.get(HEADER_LOCATOR)
        if next_locator in ("null", "none", ""):
            next_locator = None

        return response, next_locator

    def download_job_results_csv(
        self,
        sf_job_id: str,
        locator: Optional[str] = None,
        max_records: int = 10000
    ) -> Tuple[str, Optional[str]]:
        """
        Download a single page of Bulk API query CSV results as text.
        
        Returns:
            Tuple[str, Optional[str]]: (csv_content_string, next_locator)
        """
        response, next_locator = self.get_job_results_stream(
            sf_job_id=sf_job_id,
            locator=locator,
            max_records=max_records
        )
        csv_text = response.text
        logger.info(
            f"Downloaded CSV results chunk for job {sf_job_id} "
            f"(size: {len(csv_text)} bytes, next_locator: {next_locator})"
        )
        return csv_text, next_locator
