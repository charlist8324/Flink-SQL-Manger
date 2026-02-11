import sys
import os
import logging
from sqlalchemy import create_engine, text

# Add backend to path
sys.path.append(os.getcwd())

from backend.database import get_database_url

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def add_column():
    url = get_database_url()
    engine = create_engine(url)
    
    with engine.connect() as conn:
        try:
            # Check if column exists
            result = conn.execute(text("SHOW COLUMNS FROM flink_jobs LIKE 'savepoint_path'"))
            if result.fetchone():
                logger.info("Column 'savepoint_path' already exists.")
            else:
                logger.info("Adding column 'savepoint_path'...")
                conn.execute(text("ALTER TABLE flink_jobs ADD COLUMN savepoint_path VARCHAR(1024) COMMENT '启动时的Savepoint路径'"))
                conn.commit()
                logger.info("Column added successfully.")
        except Exception as e:
            logger.error(f"Error: {e}")

if __name__ == "__main__":
    add_column()
