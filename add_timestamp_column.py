
import sys
from backend.database import get_database_url
from sqlalchemy import create_engine, text

def add_column():
    url = get_database_url()
    engine = create_engine(url)
    
    # 检查 column 是否存在
    check_sql = """
    SELECT count(*) 
    FROM information_schema.columns 
    WHERE table_name = 'flink_jobs' 
    AND column_name = 'savepoint_timestamp'
    AND table_schema = DATABASE()
    """
    
    with engine.connect() as conn:
        result = conn.execute(text(check_sql)).scalar()
        if result > 0:
            print("✅ savepoint_timestamp column already exists in flink_jobs.")
        else:
            print("Adding savepoint_timestamp column to flink_jobs...")
            try:
                conn.execute(text("ALTER TABLE flink_jobs ADD COLUMN savepoint_timestamp BIGINT COMMENT 'Savepoint时间戳(毫秒)'"))
                conn.commit()
                print("✅ Added savepoint_timestamp column successfully.")
            except Exception as e:
                print(f"❌ Failed to add column: {e}")

if __name__ == "__main__":
    add_column()
