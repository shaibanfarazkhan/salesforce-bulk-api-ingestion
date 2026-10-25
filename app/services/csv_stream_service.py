"""
CSV Streaming and Chunk Parsing Service.
Streams multi-gigabyte Salesforce Bulk API query result files across paginated
locators using memory-bounded generators, maintaining constant O(1) heap space.
"""

import io
import logging
from typing import Iterator, Optional
import pandas as pd
import requests

from app.integrations.salesforce_client import SalesforceClient
from app.utils.exceptions import CSVException, DownloadException

logger = logging.getLogger(__name__)

class CSVStreamService:
    """
    Streaming consumer for Bulk API 2.0 CSV query results.
    """

    def __init__(self, salesforce_client: Optional[SalesforceClient] = None) -> None:
        self.salesforce_client = salesforce_client or SalesforceClient()

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

                logger.info(
                    f"Successfully parsed chunk #{total_chunks} "
                    f"({len(df_chunk)} rows, columns: {list(df_chunk.columns[:5])}...)"
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
                logger.info(f"Reached final cursor locator for Bulk Job {sf_job_id}. Stream complete.")
