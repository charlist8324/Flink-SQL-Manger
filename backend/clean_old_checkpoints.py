from db_operations import db_manager, FlinkCheckpoint, FlinkJob
from sqlalchemy import desc

session = db_manager.get_session()

try:
    print("=== 当前 flink_checkpoints 表中的记录 ===")
    checkpoints = session.query(FlinkCheckpoint).order_by(desc(FlinkCheckpoint.updated_at)).all()
    
    for cp in checkpoints:
        job = session.query(FlinkJob).filter(FlinkJob.job_id == cp.job_id).first()
        job_name = job.job_name if job else "未知"
        print(f"Job ID: {cp.job_id}, Job Name: {job_name}, Checkpoint ID: {cp.checkpoint_id}, Path: {cp.checkpoint_path}")
    
    print("\n=== 按作业名称分组 ===")
    from collections import defaultdict
    job_checkpoints = defaultdict(list)
    
    for cp in checkpoints:
        job = session.query(FlinkJob).filter(FlinkJob.job_id == cp.job_id).first()
        job_name = job.job_name if job else "未知"
        job_checkpoints[job_name].append(cp)
    
    for job_name, cps in job_checkpoints.items():
        print(f"\n作业名称: {job_name}")
        for cp in cps:
            print(f"  - Job ID: {cp.job_id}, Checkpoint ID: {cp.checkpoint_id}, Updated: {cp.updated_at}")
    
    print("\n=== 清理旧记录，每个作业名称只保留最新的一条 ===")
    for job_name, cps in job_checkpoints.items():
        if len(cps) > 1:
            # 保留最新的，删除其他的
            latest = cps[0]
            for old_cp in cps[1:]:
                print(f"删除旧记录: Job ID={old_cp.job_id}, Checkpoint ID={old_cp.checkpoint_id}")
                session.delete(old_cp)
    
    session.commit()
    print("\n✅ 清理完成！")
    
    print("\n=== 清理后的记录 ===")
    checkpoints = session.query(FlinkCheckpoint).all()
    for cp in checkpoints:
        job = session.query(FlinkJob).filter(FlinkJob.job_id == cp.job_id).first()
        job_name = job.job_name if job else "未知"
        print(f"Job ID: {cp.job_id}, Job Name: {job_name}, Checkpoint ID: {cp.checkpoint_id}")

finally:
    session.close()
