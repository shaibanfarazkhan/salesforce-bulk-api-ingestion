"""
CSV Streaming and Chunk Parsing Service.
Streams multi-gigabyte Salesforce Bulk API query result files across paginated
locators using memory-bounded generators, maintaining constant O(1) heap space.
"""

import io
import csv
import logging
from typing import Iterator, Optional, List, Dict, Any, Union
import pandas as pd
import requests

from app.integrations.salesforce_client import SalesforceClient
from app.utils.exceptions import CSVException, DownloadException

logger = logging.getLogger(__name__)

class CSVStreamService:
    """
    Streaming consumer and parser for Bulk API 2.0 CSV query results.
    Converts streaming raw CSV bytes/strings into Pandas DataFrames and
    structured JSON objects with memory-bounded chunk consumption.
    """

    def __init__(self, salesforce_client: Optional[SalesforceClient] = None) -> None:
        self.salesforce_client = salesforce_client or SalesforceClient()

    def parse_csv_to_records(self, csv_content: Union[str, io.StringIO]) -> List[Dict[str, Any]]:
        """
        Parse raw CSV text into a structured list of JSON/dictionary objects.
        Preserves string data integrity, handles missing values and quoted fields.
        """
        if isinstance(csv_content, str):
            if not csv_content.strip():
                return []
            f = io.StringIO(csv_content.strip())
        else:
            f = csv_content

        try:
            reader = csv.DictReader(f)
            records = [dict(row) for row in reader]
            return records
        except Exception as exc:
            logger.error(f"Failed to parse CSV string into records: {str(exc)}")
            raise CSVException(f"Failed to parse CSV string: {str(exc)}") from exc

    def stream_csv_chunks(
        self,
        sf_job_id: str,
        chunk_size: int = 10000
    ) -> Iterator[pd.DataFrame]:
        """
        Stream Bulk API 2.0 query results using locator pagination and yield Pandas DataFrame chunks.
        
        Args:
            sf_job_id: Salesforce Bulk 2.0 query job ID.
            chunk_size: Maximum rows to process per chunk (default: 10,000).
            
        Yields:
            pd.DataFrame: A chunk of up to chunk_size rows with string dtypes.
            
        Raises:
            DownloadException: If network stream fails.
            CSVException: If CSV formatting is corrupt.
        """
        locator: Optional[str] = None
        has_more_data = True
        total_chunks = 0
        total_records = 0

        logger.info(f"Initiating paginated CSV stream download for Bulk Job {sf_job_id}...")

        while has_more_data:
            total_chunks += 1
            logger.debug(f"Requesting results chunk #{total_chunks} with locator: {locator}")

            response, next_locator = self.salesforce_client.get_job_results_stream(
                sf_job_id=sf_job_id,
                locator=locator,
                max_records=chunk_size
            )

            # Consume chunk line-by-line from socket stream
            try:
                # Read all lines for this locator response
                lines = []
                for raw_line in response.iter_lines(decode_unicode=True):
                    if raw_line:
                        lines.append(raw_line)

                if not lines:
                    logger.info(f"Received empty stream chunk for locator {locator}. Stream completed.")
                    break

                # The first line is the CSV header for this result
                csv_buffer = io.StringIO("\n".join(lines))

                # Parse into Pandas DataFrame with all string types to preserve precision and IDs
                df_chunk = pd.read_csv(
                    csv_buffer,
                    dtype=str,
                    keep_default_na=False,
                    na_values=["#N/A", "#N/A N/A", "#NA", "-1.#IND", "-1.#QNAN", "-NaN", "-nan", "1.#IND", "1.#QNAN"]
                )

                chunk_len = len(df_chunk)
                total_records += chunk_len
                logger.info(
                    f"Successfully parsed chunk #{total_chunks} "
                    f"({chunk_len} rows, total so far: {total_records})"
                )
                yield df_chunk

            except Exception as exc:
                logger.error(f"Failed to parse CSV stream chunk #{total_chunks}: {str(exc)}")
                raise CSVException(
                    message=f"Failed to parse Bulk API CSV result stream: {str(exc)}",
                    details={"chunk_number": total_chunks, "locator": locator}
                ) from exc

            # Prepare for next locator
            locator = next_locator
            if not locator or locator.lower() in ("null", "none"):
                has_more_data = False
                logger.info(
                    f"Reached final cursor locator for Bulk Job {sf_job_id}. "
                    f"Stream complete: {total_chunks} chunk(s), {total_records} record(s)."
                )

    def stream_json_records(
        self,
        sf_job_id: str,
        chunk_size: int = 10000
    ) -> Iterator[List[Dict[str, Any]]]:
        """
        Stream Bulk API 2.0 query results and yield chunks as structured JSON/dictionary objects.
        """
        for df_chunk in self.stream_csv_chunks(sf_job_id=sf_job_id, chunk_size=chunk_size):
            records = df_chunk.to_dict(orient="records")
            yield records

    def download_all_records_as_json(
        self,
        sf_job_id: str,
        max_records: Optional[int] = None,
        chunk_size: int = 10000
    ) -> List[Dict[str, Any]]:
        """
        Download all query results across all paginated locators and return as structured JSON records.
        """
        all_records: List[Dict[str, Any]] = []
        logger.info(f"Downloading all structured records for Bulk Job {sf_job_id}...")

        for chunk in self.stream_json_records(sf_job_id=sf_job_id, chunk_size=chunk_size):
            all_records.extend(chunk)
            if max_records and len(all_records) >= max_records:
                all_records = all_records[:max_records]
                logger.info(f"Reached requested max_records ceiling ({max_records}). Stopping download.")
                break

        logger.info(f"Downloaded total {len(all_records)} structured JSON records for Bulk Job {sf_job_id}.")
        return all_records
