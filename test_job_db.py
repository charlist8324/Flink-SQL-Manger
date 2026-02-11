"""测试数据库中是否存在作业"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "backend"))

from database import db_settings
import pymysql

print(f"MySQL连接: {db_settings.DB_HOST}:{db_settings.DB_PORT}/{db_settings.DB_NAME}")

try:
    conn = pymysql.connect(
        host=db_settings.DB_HOST,
        port=db_settings.DB_PORT,
        user=db_settings.DB_USER,
        password=db_settings.DB_PASSWORD,
        database=db_settings.DB_NAME,
        charset='utf8mb4'
    )
    cursor = conn.cursor()
    
    # 查询job_id
    cursor.execute("SELECT COUNT(*) FROM flink_jobs WHERE job_id LIKE %s", ('663671524b8312a8ae02dbd0ce697d0b',))
    count = cursor.fetchone()[0]
    print(f"数据库中有 {count} 个匹配的作业")
    
    # 显示详细信息
    if count > 0:
        cursor.execute("SELECT job_id, job_name, state, created_at FROM flink_jobs WHERE job_id LIKE %s ORDER BY created_at DESC", ('663671524b8312a8ae02dbd0ce697d0b',))
        results = cursor.fetchall()
        print(f"\n作业详细信息:")
        print(f"{'Job ID':<20} | {'作业名称':<30} | {'状态':<15} | {'创建时间':<20}")
        print("-" * 80)
        for row in results:
            print(f"{row[0]:<37} | {row[1]:<30} | {row[2]:<15} | {row[3]}")
    else:
        print(f"数据库中没有找到此作业")
    
    cursor.close()
    conn.close()
    
    print(f"\n测试完成！")
    
except Exception as e:
    print(f"数据库连接或查询失败: {e}")
