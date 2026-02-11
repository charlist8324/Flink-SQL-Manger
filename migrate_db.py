
from backend.database import get_database_url
from sqlalchemy import create_engine, text
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def migrate_db():
    try:
        db_url = get_database_url()
        engine = create_engine(db_url)
        
        with engine.connect() as conn:
            # Check if properties column exists
            try:
                result = conn.execute(text("SHOW COLUMNS FROM flink_jobs LIKE 'properties'"))
                if result.fetchone():
                    logger.info("Column 'properties' already exists.")
                else:
                    logger.info("Adding column 'properties' to flink_jobs table...")
                    conn.execute(text("ALTER TABLE flink_jobs ADD COLUMN properties JSON COMMENT '配置属性'"))
                    conn.commit()
                    logger.info("Column 'properties' added successfully.")
            except Exception as e:
                logger.error(f"Error checking/adding column: {e}")

    except Exception as e:
        logger.error(f"Migration failed: {e}")

if __name__ == "__main__":
    migrate_db()
