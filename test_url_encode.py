"""
简单测试数据库连接URL编码
"""
from urllib.parse import quote_plus

# 测试密码编码
password = "Admin@900"
encoded_password = quote_plus(password)

print("=== 密码编码测试 ===")
print(f"原始密码: {password}")
print(f"编码后密码: {encoded_password}")
print()

# 测试完整URL
db_user = "root"
db_host = "10.178.80.101"
db_port = 3306
db_name = "flink_db"

# 错误的URL（未编码）
wrong_url = f"mysql+pymysql://{db_user}:{password}@{db_host}:{db_port}/{db_name}?charset=utf8mb4"
print("=== 错误的URL（未编码）===")
print(wrong_url)
print()

# 正确的URL（已编码）
correct_url = f"mysql+pymysql://{db_user}:{encoded_password}@{db_host}:{db_port}/{db_name}?charset=utf8mb4"
print("=== 正确的URL（已编码）===")
print(correct_url)
print()

print("✅ 密码中的 @ 符号已正确编码为 %40")
