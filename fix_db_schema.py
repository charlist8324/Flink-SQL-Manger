import sys
import os

# Add current directory to path so we can import backend
sys.path.append(os.getcwd())

from backend.db_operations import db_manager
from sqlalchemy import text

def fix_schema():
    print("Initializing database connection...")
    db_manager.initialize()
    session = db_manager.get_session()
    try:
        # 1. Fix flink_jobs table
        print("Checking flink_jobs table...")
        result = session.execute(text("SHOW COLUMNS FROM flink_jobs LIKE 'savepoint_timestamp'"))
        if result.fetchone():
            print("✅ Column 'savepoint_timestamp' already exists in flink_jobs.")
        else:
            print("⚠️ Column 'savepoint_timestamp' missing in flink_jobs. Adding it...")
            session.execute(text("ALTER TABLE flink_jobs ADD COLUMN savepoint_timestamp BIGINT COMMENT 'Savepoint时间戳(毫秒)'"))
            session.commit()
            print("✅ Column 'savepoint_timestamp' added to flink_jobs.")

        # 2. Fix flink_savepoints table (check for timestamp column just in case)
        print("Checking flink_savepoints table...")
        result = session.execute(text("SHOW COLUMNS FROM flink_savepoints LIKE 'timestamp'"))
        if result.fetchone():
            print("✅ Column 'timestamp' already exists in flink_savepoints.")
        else:
            print("⚠️ Column 'timestamp' missing in flink_savepoints. Adding it...")
            session.execute(text("ALTER TABLE flink_savepoints ADD COLUMN timestamp BIGINT COMMENT '时间戳(毫秒)'"))
            session.commit()
            print("✅ Column 'timestamp' added to flink_savepoints.")

    except Exception as e:
        print(f"❌ Error during migration: {e}")
        session.rollback()
    finally:
        session.close()
        print("Schema check completed.")

if __name__ == "__main__":
    fix_schema()
