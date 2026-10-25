"""
Phase 5 Salesforce Authentication Verification Script root wrapper.
Invokes scripts/verify_phase5_authentication.py execution logic.
"""

import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from scripts.verify_phase5_authentication import run_phase5_verification

if __name__ == "__main__":
    if not run_phase5_verification():
        sys.exit(1)
    sys.exit(0)
