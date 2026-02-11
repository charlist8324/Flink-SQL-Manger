
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import os

# Assuming the database URL is the same as in the app
# I'll check main.py or db_operations.py to find the DB URL
# For now, I'll try to deduce it or import it.
# db_operations.py imports SessionLocal.

import sys
sys.path.append(os.getcwd())
from backend.db_operations import SessionLocal, FlinkJob

def check_jobs():
    db = SessionLocal()
    try:
        # Check for AAA job
        jobs = db.query(FlinkJob).filter(FlinkJob.job_name == "AAA").all()
        print(f"Found {len(jobs)} jobs named AAA")
        for job in jobs:
            print(f"Job ID: {job.job_id}")
            print(f"SQL Text: {job.sql_text}")
            print("-" * 20)
        
        # Check for any job with mysql-cdc
        jobs_cdc = db.query(FlinkJob).filter(FlinkJob.sql_text.like("%mysql-cdc%")).all()
        print(f"Found {len(jobs_cdc)} jobs with mysql-cdc")
        for job in jobs_cdc:
            print(f"Job ID: {job.job_id}")
            print(f"Job Name: {job.job_name}")
            print(f"SQL Text: {job.sql_text}")
            print("-" * 20)

    except Exception as e:
        print(f"Error: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    check_jobs()
