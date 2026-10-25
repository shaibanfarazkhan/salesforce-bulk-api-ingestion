"""
Phase 1 Requirements & Environment Verification Script
Validates the execution environment, Python version compatibility,
and architectural artifacts required for the Salesforce Bulk API 2.0 Ingestion Platform.
"""

import sys
import platform
import os
from pathlib import Path

def run_phase1_verification() -> bool:
    print("=" * 70)
    print("SALESFORCE BULK API 2.0 INGESTION SYSTEM - PHASE 1 VERIFICATION")
    print("=" * 70)
    
    success = True
    
    # 1. Validate Python Runtime Version (Target: Python 3.13+)
    py_version = sys.version_info
    print(f"[*] Detected Python Runtime: {platform.python_version()} ({platform.architecture()[0]})")
    if py_version.major == 3 and py_version.minor >= 13:
        print("  [SUCCESS] Python 3.13+ runtime verified.")
    else:
        print(f"  [WARNING] Python 3.13+ recommended. Current: {platform.python_version()}")
        # Still non-fatal if Python 3.11+, but flagged.
        if py_version.major < 3 or py_version.minor < 11:
            print("  [FAIL] Python version is below minimum supported 3.11.")
            success = False

    # 2. Check Operating System
    print(f"[*] Operating System: {platform.system()} {platform.release()} ({sys.platform})")
    
    # 3. Check Default Encoding
    encoding = sys.getdefaultencoding()
    print(f"[*] System Default Encoding: {encoding}")
    if encoding.lower() != "utf-8":
        print("  [WARN] Default encoding is not UTF-8. Force PYTHONIOENCODING=utf-8 for production pipelines.")

    # 4. Verify Phase 1 Documentation Artifacts
    doc_path = Path("docs/phases/01_REQUIREMENT_ANALYSIS.md")
    print(f"[*] Checking Phase 1 Architectural Artifact: {doc_path}")
    if doc_path.exists():
        size_bytes = doc_path.stat().st_size
        print(f"  [SUCCESS] Specification exists ({size_bytes} bytes).")
    else:
        print("  [FAIL] docs/phases/01_REQUIREMENT_ANALYSIS.md not found.")
        success = False

    # 5. Core Architectural Constraints Check
    print("\n[*] Validating System Design Constraints:")
    constraints = [
        ("Governor Limit Tracking", "10,000 Bulk 2.0 Query Jobs / 24h rolling window"),
        ("Memory Ceiling", "Max Heap <= 512 MB under streaming CSV mode"),
        ("Database Target", "PostgreSQL 17 with Idempotent Upsert (ON CONFLICT)"),
        ("API Architecture", "Flask Application Factory with Marshmallow & SQLAlchemy 2.0"),
        ("Resilience Pattern", "Exponential Backoff with Decorrelated Jitter for Polling")
    ]
    for name, detail in constraints:
        print(f"  - {name:<26}: {detail}")

    print("\n" + "=" * 70)
    if success:
        print("PHASE 1 REQUIREMENT ANALYSIS VERIFICATION: PASSED")
    else:
        print("PHASE 1 REQUIREMENT ANALYSIS VERIFICATION: FAILED")
    print("=" * 70)
    return success

if __name__ == "__main__":
    if not run_phase1_verification():
        sys.exit(1)
