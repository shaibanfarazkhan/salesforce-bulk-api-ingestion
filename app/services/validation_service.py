"""
Data Cleansing and Schema Validation Service.
Enforces domain constraints, 18-character Salesforce ID validation,
ISO timestamp normalization to UTC, numeric/boolean coercion,
duplicate deduplication, and dead-letter quarantine routing.
"""

import re
import logging
from typing import Dict, List, Tuple, Any, Optional
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import pandas as pd

from app.utils.exceptions import ValidationException

logger = logging.getLogger(__name__)

# Salesforce 15 or 18-character alphanumeric ID pattern
SALESFORCE_ID_REGEX = re.compile(r"^[a-zA-Z0-9]{15}([a-zA-Z0-9]{3})?$")
EMAIL_REGEX = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

class ValidationService:
    """
    Validates, sanitizes, and partitions raw CSV chunks into accepted and quarantined records.
    """

    def sanitize_and_validate(
        self,
        df_chunk: pd.DataFrame,
        target_object: str
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Validate records in DataFrame chunk.
        
        Args:
            df_chunk: Pandas DataFrame chunk of string records.
            target_object: Target CRM entity ('Account', 'Contact', 'Opportunity').
            
        Returns:
            Tuple[List[Dict], List[Dict]]: (accepted_records, rejected_records)
        """
        accepted: List[Dict[str, Any]] = []
        rejected: List[Dict[str, Any]] = []
        seen_ids = set()

        # Normalize column names to lowercase for robust mapping
        col_map = {c: c.lower().replace("_", "") for c in df_chunk.columns}

        for idx, row in df_chunk.iterrows():
            raw_row = row.to_dict()
            try:
                cleaned_record = self._validate_and_transform_row(raw_row, col_map, target_object, seen_ids)
                accepted.append(cleaned_record)
                seen_ids.add(cleaned_record["salesforce_id"])
            except ValidationException as val_exc:
                sf_id = raw_row.get("Id") or raw_row.get("ID") or raw_row.get("id") or "UNKNOWN"
                rejected.append({
                    "sf_record_id": sf_id if len(sf_id) <= 18 else sf_id[:18],
                    "target_object": target_object,
                    "raw_payload": raw_row,
                    "error_code": val_exc.error_code,
                    "error_message": val_exc.message
                })

        logger.info(
            f"Validation complete for {target_object} chunk: "
            f"{len(accepted)} accepted, {len(rejected)} rejected."
        )
        return accepted, rejected

    def _validate_and_transform_row(
        self,
        raw: Dict[str, Any],
        col_map: Dict[str, str],
        target_object: str,
        seen_ids: set
    ) -> Dict[str, Any]:
        """Validate an individual record according to entity rules."""
        # 1. Extract and Validate Salesforce ID
        sf_id = self._get_field(raw, ["Id", "ID", "id"])
        if not sf_id:
            raise ValidationException("Missing mandatory Primary Key field 'Id'")
        
        sf_id = sf_id.strip()
        if not SALESFORCE_ID_REGEX.match(sf_id):
            raise ValidationException(f"Invalid Salesforce ID format: '{sf_id}'")

        if sf_id in seen_ids:
            raise ValidationException(f"Duplicate Salesforce ID detected within the same batch: '{sf_id}'")

        # 2. Extract and Validate SystemModstamp
        modstamp_str = self._get_field(raw, ["SystemModstamp", "systemmodstamp", "system_modstamp", "LastModifiedDate"])
        if not modstamp_str:
            raise ValidationException("Missing mandatory audit timestamp 'SystemModstamp'")
        
        system_modstamp = self._parse_iso_datetime(modstamp_str)
        if not system_modstamp:
            raise ValidationException(f"Invalid timestamp format for SystemModstamp: '{modstamp_str}'")

        # Optional sf_created_date
        created_str = self._get_field(raw, ["CreatedDate", "createddate", "created_date"])
        sf_created_date = self._parse_iso_datetime(created_str) if created_str else None

        # Soft Delete Flag
        is_del_str = self._get_field(raw, ["IsDeleted", "isdeleted", "is_deleted"]) or "false"
        is_deleted = is_del_str.strip().lower() in ("true", "1", "t", "yes")

        record: Dict[str, Any] = {
            "salesforce_id": sf_id,
            "system_modstamp": system_modstamp,
            "sf_created_date": sf_created_date,
            "is_deleted": is_deleted,
            "synced_at": datetime.now(timezone.utc)
        }

        # 3. Entity-Specific Field Normalization
        obj_type = target_object.lower()
        if "account" in obj_type:
            name = self._get_field(raw, ["Name", "name"])
            if not name or not name.strip():
                raise ValidationException("Missing mandatory required field 'Name' on Account")
            record["name"] = name.strip()
            record["account_number"] = self._clean_str(self._get_field(raw, ["AccountNumber", "accountnumber"]))
            record["account_type"] = self._clean_str(self._get_field(raw, ["Type", "type", "AccountType"]))
            record["industry"] = self._clean_str(self._get_field(raw, ["Industry", "industry"]))
            record["annual_revenue"] = self._parse_decimal(self._get_field(raw, ["AnnualRevenue", "annualrevenue"]), "AnnualRevenue")
            record["phone"] = self._clean_str(self._get_field(raw, ["Phone", "phone"]))
            record["website"] = self._clean_str(self._get_field(raw, ["Website", "website"]))
            record["billing_city"] = self._clean_str(self._get_field(raw, ["BillingCity", "billingcity"]))
            record["billing_country"] = self._clean_str(self._get_field(raw, ["BillingCountry", "billingcountry"]))

        elif "contact" in obj_type:
            last_name = self._get_field(raw, ["LastName", "lastname", "last_name"])
            if not last_name or not last_name.strip():
                raise ValidationException("Missing mandatory required field 'LastName' on Contact")
            record["last_name"] = last_name.strip()
            record["first_name"] = self._clean_str(self._get_field(raw, ["FirstName", "firstname", "first_name"]))
            record["account_id"] = self._clean_sf_id(self._get_field(raw, ["AccountId", "accountid", "account_id"]))
            email = self._clean_str(self._get_field(raw, ["Email", "email"]))
            if email and not EMAIL_REGEX.match(email):
                logger.warning(f"Malformed contact email '{email}' for ID {sf_id}; retaining raw string.")
            record["email"] = email
            record["phone"] = self._clean_str(self._get_field(raw, ["Phone", "phone"]))
            record["title"] = self._clean_str(self._get_field(raw, ["Title", "title"]))
            record["department"] = self._clean_str(self._get_field(raw, ["Department", "department"]))
            record["lead_source"] = self._clean_str(self._get_field(raw, ["LeadSource", "leadsource"]))

        elif "opportunity" in obj_type:
            name = self._get_field(raw, ["Name", "name"])
            if not name or not name.strip():
                raise ValidationException("Missing mandatory required field 'Name' on Opportunity")
            stage = self._get_field(raw, ["StageName", "stagename", "stage_name"])
            if not stage or not stage.strip():
                raise ValidationException("Missing mandatory required field 'StageName' on Opportunity")
            record["name"] = name.strip()
            record["stage_name"] = stage.strip()
            record["account_id"] = self._clean_sf_id(self._get_field(raw, ["AccountId", "accountid", "account_id"]))
            record["amount"] = self._parse_decimal(self._get_field(raw, ["Amount", "amount"]), "Amount")
            record["probability"] = self._parse_decimal(self._get_field(raw, ["Probability", "probability"]), "Probability")
            close_str = self._get_field(raw, ["CloseDate", "closedate", "close_date"])
            record["close_date"] = self._parse_date(close_str) if close_str else None
            record["opportunity_type"] = self._clean_str(self._get_field(raw, ["Type", "type", "OpportunityType"]))

        return record

    @staticmethod
    def _get_field(raw: Dict[str, Any], candidate_keys: List[str]) -> Optional[str]:
        for k in candidate_keys:
            if k in raw and raw[k] is not None:
                val = str(raw[k]).strip()
                if val != "":
                    return val
        return None

    @staticmethod
    def _clean_str(val: Optional[str]) -> Optional[str]:
        if val is None or val.strip() == "":
            return None
        return val.strip()

    @staticmethod
    def _clean_sf_id(val: Optional[str]) -> Optional[str]:
        if not val or not val.strip():
            return None
        clean = val.strip()
        if len(clean) in (15, 18):
            return clean
        return None

    @staticmethod
    def _parse_decimal(val: Optional[str], field_name: str) -> Optional[Decimal]:
        if not val or val.strip() == "":
            return None
        clean_val = val.replace(",", "").replace("$", "").strip()
        try:
            d = Decimal(clean_val)
            if d < 0:
                raise ValidationException(f"Negative value not allowed for column '{field_name}': {val}")
            return d
        except InvalidOperation as exc:
            raise ValidationException(f"Invalid numeric decimal format for column '{field_name}': '{val}'") from exc

    @staticmethod
    def _parse_iso_datetime(val: Optional[str]) -> Optional[datetime]:
        if not val or not val.strip():
            return None
        clean = val.strip()
        # Handle formats: 2026-10-24T20:30:00.000+0000 or 2026-10-24T20:30:00Z
        for fmt in (
            "%Y-%m-%dT%H:%M:%S.%f%z",
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%dT%H:%M:%S.%fZ",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d"
        ):
            try:
                dt = datetime.strptime(clean, fmt)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except ValueError:
                continue
        return None

    @staticmethod
    def _parse_date(val: Optional[str]):
        if not val or not val.strip():
            return None
        dt = ValidationService._parse_iso_datetime(val)
        return dt.date() if dt else None
