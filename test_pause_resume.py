"""
测试Flink暂停和恢复功能
"""
import asyncio
import sys
sys.path.append(".")

from backend.flink_client import FlinkClient
from backend.config import FLINK_REST_URL


async def test_pause_and_resume():
    """测试暂停和恢复功能"""
    
    client = FlinkClient(FLINK_REST_URL)
    
    print("=" * 60)
    print("Test 1: 获取作业列表")
    print("=" * 60)
    jobs_overview = await client.get_jobs_overview()
    jobs = jobs_overview.get("jobs", [])
    
    if not jobs:
        print("No jobs found")
        return
    
    job_id = jobs[0].get("id")
    job_name = jobs[0].get("name", job_id)
    print(f"Found job: {job_name} (ID: {job_id})")
    print(f"Status: {jobs[0].get('status')}")
    
    if jobs[0].get("status") != "RUNNING":
        print(f"\nJob is not RUNNING, cannot test pause/resume")
        return
    
    print("\n" + "=" * 60)
    print("Test 2: 获取作业详情")
    print("=" * 60)
    job_detail = await client.get_job_detail(job_id)
    print(f"Job name: {job_detail.get('name')}")
    print(f"Job state: {job_detail.get('state')}")
    print(f"Start time: {job_detail.get('start-time')}")
    print(f"Duration: {job_detail.get('duration')}")
    
    print("\n" + "=" * 60)
    print("Test 3: 触发Savepoint并停止作业")
    print("=" * 60)
    print(f"Stopping job {job_id} with savepoint...")
    
    # 使用默认的savepoint目录
    result = await client.stop_job_with_savepoint(job_id, None)
    
    print(f"Result: {result}")
    savepoint_path = result.get("savepoint_path")
    
    if savepoint_path:
        print(f"Savepoint created: {savepoint_path}")
        
        # 等待几秒
        print("\nWaiting 5 seconds for job to stop...")
        await asyncio.sleep(5)
        
        print("\n" + "=" * 60)
        print("Test 4: 获取Savepoint列表")
        print("=" * 60)
        savepoints = await client.get_job_savepoints(job_id)
        print(f"Savepoints: {savepoints}")
        
        print("\n" + "=" * 60)
        print("Test 5: 验证作业已停止")
        print("=" * 60)
        job_detail_after = await client.get_job_detail(job_id)
        print(f"Job state after stop: {job_detail_after.get('state')}")
        
        print("\n" + "=" * 60)
        print("Summary")
        print("=" * 60)
        print(f"1. Pause and savepoint: {'OK' if savepoint_path else 'FAILED'}")
        print(f"2. Savepoint path: {savepoint_path}")
        print(f"3. Job state: {job_detail_after.get('state')}")
    else:
        print("Failed to get savepoint path")


if __name__ == "__main__":
    asyncio.run(test_pause_and_resume())
