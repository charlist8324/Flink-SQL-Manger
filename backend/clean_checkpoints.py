from db_operations import db_manager, FlinkCheckpoint
from sqlalchemy import select, func

session = db_manager.get_session()

try:
    # 获取所有checkpoint记录
    all_checkpoints = session.query(FlinkCheckpoint).all()
    
    # 按job_id分组
    job_checkpoints = {}
    for cp in all_checkpoints:
        if cp.job_id not in job_checkpoints:
            job_checkpoints[cp.job_id] = []
        job_checkpoints[cp.job_id].append(cp)
    
    # 对每个作业，只保留最新的checkpoint
    deleted_count = 0
    for job_id, checkpoints in job_checkpoints.items():
        if len(checkpoints) > 1:
            # 按checkpoint_id降序排序，保留最新的
            checkpoints.sort(key=lambda x: x.checkpoint_id, reverse=True)
            # 删除除了第一个之外的所有记录
            for cp in checkpoints[1:]:
                session.delete(cp)
                deleted_count += 1
                print(f"删除作业 {job_id} 的旧Checkpoint #{cp.checkpoint_id}")
    
    session.commit()
    print(f"✅ 已清理 {deleted_count} 条旧checkpoint记录")
    
    # 查看剩余的checkpoint记录
    remaining = session.query(FlinkCheckpoint).all()
    print(f"✅ 剩余 {len(remaining)} 条checkpoint记录:")
    for cp in remaining:
        print(f"  - JobID: {cp.job_id}, CheckpointID: {cp.checkpoint_id}")
        
finally:
    session.close()
