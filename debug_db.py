
from backend.db_operations import get_all_jobs
import json
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)

try:
    print("Fetching jobs from DB...")
    jobs = get_all_jobs(limit=100)
    print(f"Found {len(jobs)} jobs in DB.")
    for job in jobs:
        print(json.dumps(job, default=str, indent=2))
except Exception as e:
    print(f"Error: {e}")
