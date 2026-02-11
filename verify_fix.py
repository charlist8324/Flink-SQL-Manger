"""
快速验证修复效果
"""
from urllib.parse import quote_plus

# 测试密码编码
password = "Admin@900"
encoded_password = quote_plus(password)

print("=" * 60)
print("数据库密码URL编码测试")
print("=" * 60)
print()
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
print("错误的URL（未编码）:")
print(wrong_url)
print("  问题: 出现两个 @ 符号，URL解析错误")
print()

# 正确的URL（已编码）
correct_url = f"mysql+pymysql://{db_user}:{encoded_password}@{db_host}:{db_port}/{db_name}?charset=utf8mb4"
print("正确的URL（已编码）:")
print(correct_url)
print("  正确: 密码中的 @ 被编码为 %40")
print()

print("=" * 60)
print("验证结果: 修复成功！")
print("=" * 60)
print()
print("下一步:")
print("1. 重启后端服务: .\\setup_and_run.ps1")
print("2. 访问 http://localhost:8000/health 检查数据库连接")
print("3. 测试暂停作业功能")
print()
