"""
测试数据库查询，检查 flink_jobs 表数据
"""
import sys
from pathlib import Path

# 添加 backend 目录到 Python 路径
backend_dir = Path(__file__).parent / "backend"
sys.path.insert(0, str(backend_dir))

print("=" * 70)
print("测试数据库查询 - flink_jobs 表")
print("=" * 70)
print()

try:
    from db_operations import get_all_jobs, get_job
    
    # 测试1: 获取所有作业
    print("测试 1: 获取所有作业")
    print("-" * 70)
    all_jobs = get_all_jobs(state=None, limit=100)
    print(f"数据库中共有 {len(all_jobs)} 个作业")
    print()
    
    if all_jobs:
        print("作业列表:")
        for i, job in enumerate(all_jobs[:10], 1):  # 只显示前10个
            print(f"  {i}. job_id: {job.get('job_id')}")
            print(f"     job_name: {job.get('job_name')}")
            print(f"     state: {job.get('state')}")
            print(f"     start_time: {job.get('start_time')}")
            print(f"     end_time: {job.get('end_time')}")
            print(f"     duration: {job.get('duration')}")
            print(f"     created_at: {job.get('created_at')}")
            print()
        
        if len(all_jobs) > 10:
            print(f"  ... 还有 {len(all_jobs) - 10} 个作业")
        print()
        
        # 测试2: 统计各状态的作业数量
        print("测试 2: 统计各状态的作业数量")
        print("-" * 70)
        state_count = {}
        for job in all_jobs:
            state = job.get('state', 'UNKNOWN')
            state_count[state] = state_count.get(state, 0) + 1
        
        for state, count in sorted(state_count.items()):
            print(f"  {state}: {count} 个作业")
        print()
        
        # 测试3: 检查是否有 RUNNING 状态的作业
        print("测试 3: 检查 RUNNING 状态的作业")
        print("-" * 70)
        running_jobs = [job for job in all_jobs if job.get('state') == 'RUNNING']
        print(f"RUNNING 状态的作业: {len(running_jobs)} 个")
        for job in running_jobs:
            print(f"  - {job.get('job_id')}: {job.get('job_name')}")
        print()
        
        # 测试4: 检查非 RUNNING 状态的作业（应该显示在历史作业中）
        print("测试 4: 非 RUNNING 状态的作业（应该显示在历史作业中）")
        print("-" * 70)
        history_jobs = [job for job in all_jobs if job.get('state') != 'RUNNING']
        print(f"非 RUNNING 状态的作业: {len(history_jobs)} 个")
        for job in history_jobs[:10]:
            print(f"  - {job.get('job_id')}: {job.get('job_name')} ({job.get('state')})")
        print()
        
    else:
        print("  数据库中没有作业！")
        print()
        print("可能的原因：")
        print("  1. 数据库连接失败")
        print("  2. flink_jobs 表为空")
        print("  3. 数据库配置错误")
        print()
    
    print("=" * 70)
    print("测试完成")
    print("=" * 70)
    
except Exception as e:
    print(f"测试失败: {e}")
    import traceback
    traceback.print_exc()
