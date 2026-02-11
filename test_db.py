"""
测试数据库连接脚本
"""
import sys
sys.path.append(".")

from backend.database import db_settings, get_database_url

print("=" * 60)
print("Database Configuration")
print("=" * 60)
print(f"Host: {db_settings.DB_HOST}")
print(f"Port: {db_settings.DB_PORT}")
print(f"User: {db_settings.DB_USER}")
print(f"Database: {db_settings.DB_NAME}")
print(f"URL: {get_database_url()}")
print("=" * 60)

try:
    import pymysql
    
    print("\nTesting database connection...")
    connection = pymysql.connect(
        host=db_settings.DB_HOST,
        port=db_settings.DB_PORT,
        user=db_settings.DB_USER,
        password=db_settings.DB_PASSWORD,
        database=db_settings.DB_NAME,
        charset='utf8mb4'
    )
    
    print("Database connection successful!")
    
    # Test query
    with connection.cursor() as cursor:
        cursor.execute("SELECT VERSION()")
        version = cursor.fetchone()
        print(f"MySQL version: {version[0]}")
        
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        print(f"\nTables in database: {len(tables)}")
        for table in tables:
            print(f"  - {table[0]}")
    
    connection.close()
    print("\nTest completed!")
    
except Exception as e:
    print(f"Database connection failed: {e}")
    print("\nPlease check:")
    print("1. MySQL service is running")
    print("2. Database configuration is correct")
    print("3. Network connection is available")
    sys.exit(1)
