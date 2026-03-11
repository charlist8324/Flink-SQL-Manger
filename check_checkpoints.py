from backend.db_operations import db_manager, FlinkCheckpoint
from sqlalchemy import select

session = db_manager.get_session()
result = session.execute(select(FlinkCheckpoint).order_by(FlinkCheckpoint.id.desc()).limit(5))
checkpoints = result.scalars().all()

print(f"找到 {len(checkpoints)} 条Checkpoint记录:")
for c in checkpoints:
    print(f"ID: {c.id}, JobID: {c.job_id}, CheckpointID: {c.checkpoint_id}, Path: {c.checkpoint_path}, Status: {c.status}, TriggerTime: {c.trigger_time}")

session.close()
