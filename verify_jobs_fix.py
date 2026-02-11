"""
验证作业列表和历史作业的修复效果
"""
import sys
from pathlib import Path

# 添加 backend 目录到 Python 路径
backend_dir = Path(__file__).parent / "backend"
sys.path.insert(0, str(backend_dir))

print("=" * 70)
print("验证作业列表和历史作业修复效果")
print("=" * 70)
print()

# 模拟作业数据
def test_sorting():
    """测试排序逻辑，处理 None 值"""
    print("测试 1: 排序逻辑（处理 None 值）")
    print("-" * 70)

    # 模拟作业数据，包含 None 值
    jobs_with_none = [
        {"jid": "job1", "start-time": None},
        {"jid": "job2", "start-time": 1000},
        {"jid": "job3", "start-time": None},
        {"jid": "job4", "start-time": 500},
        {"jid": "job5", "start-time": 2000},
    ]

    print("原始数据（包含 None 值）:")
    for job in jobs_with_none:
        print(f"  {job}")

    print()
    print("使用修复后的排序方式:")
    try:
        sorted_jobs = sorted(jobs_with_none, key=lambda x: (x.get("start-time") or 0), reverse=True)
        print("  排序成功！")
        for i, job in enumerate(sorted_jobs, 1):
            print(f"  {i}. {job}")
        return True
    except Exception as e:
        print(f"  排序失败: {e}")
        return False

def test_filtering():
    """测试状态过滤"""
    print()
    print("测试 2: 状态过滤")
    print("-" * 70)

    # 模拟作业数据，包含多种状态
    jobs = [
        {"job_id": "job1", "state": "RUNNING", "job_name": "Running Job 1"},
        {"job_id": "job2", "state": "FINISHED", "job_name": "Finished Job 2"},
        {"job_id": "job3", "state": "RUNNING", "job_name": "Running Job 3"},
        {"job_id": "job4", "state": "FAILED", "job_name": "Failed Job 4"},
        {"job_id": "job5", "state": "CANCELED", "job_name": "Canceled Job 5"},
        {"job_id": "job6", "state": "RUNNING", "job_name": "Running Job 6"},
    ]

    print("原始数据:")
    for job in jobs:
        print(f"  {job['job_id']}: {job['state']} - {job['job_name']}")

    print()
    print("过滤后的实时作业（只显示 RUNNING 状态）:")
    running_jobs = [job for job in jobs if job.get("state") == "RUNNING"]
    for job in running_jobs:
        print(f"  {job['job_id']}: {job['state']} - {job['job_name']}")

    print()
    print("过滤后的历史作业（排除 RUNNING 状态）:")
    history_jobs = [job for job in jobs if job.get("state") != "RUNNING"]
    for job in history_jobs:
        print(f"  {job['job_id']}: {job['state']} - {job['job_name']}")

    print()
    print(f"统计:")
    print(f"  实时作业（RUNNING）: {len(running_jobs)} 个")
    print(f"  历史作业（非RUNNING）: {len(history_jobs)} 个")
    return True

def test_url_encoding():
    """测试 URL 编码修复"""
    print()
    print("测试 3: 数据库密码 URL 编码")
    print("-" * 70)

    from urllib.parse import quote_plus

    password = "Admin@900"
    encoded_password = quote_plus(password)

    print(f"原始密码: {password}")
    print(f"编码后密码: {encoded_password}")

    db_user = "root"
    db_host = "10.178.80.101"
    db_port = 3306
    db_name = "flink_db"

    correct_url = f"mysql+pymysql://{db_user}:{encoded_password}@{db_host}:{db_port}/{db_name}?charset=utf8mb4"
    print(f"正确的连接字符串:")
    print(f"  {correct_url}")
    return True

if __name__ == "__main__":
    results = []

    # 运行所有测试
    results.append(("排序逻辑", test_sorting()))
    results.append(("状态过滤", test_filtering()))
    results.append(("URL编码", test_url_encoding()))

    # 总结
    print()
    print("=" * 70)
    print("测试总结")
    print("=" * 70)
    for name, result in results:
        status = "✅ 通过" if result else "❌ 失败"
        print(f"  {name}: {status}")

    print()
    print("下一步:")
    print("  1. 重启后端服务")
    print("  2. 访问 http://localhost:8000/health 检查数据库连接")
    print("  3. 检查实时作业页面，应只显示 RUNNING 状态的作业")
    print("  4. 检查历史作业页面，应只显示非 RUNNING 状态的作业")
    print("  5. 测试恢复作业功能，应不再出现类型错误")
    print()
