"""
Seed Data Script wrapper in scripts directory.
Invokes the root seed_data.py execution logic.
"""

import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from seed_data import seed_database

if __name__ == "__main__":
    drop_flag = "--drop" in sys.argv
    if not seed_database(drop_existing=drop_flag):
        sys.exit(1)
    sys.exit(0)
