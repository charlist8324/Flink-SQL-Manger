from db_operations import db_manager, FlinkConfig, save_flink_config
from sqlalchemy import select

session = db_manager.get_session()

try:
    # 检查是否已存在checkpoint_sync_interval配置
    existing = session.query(FlinkConfig).filter(
        FlinkConfig.config_key == "checkpoint_sync_interval"
    ).first()

    if existing:
        print(f"✅ 配置已存在: checkpoint_sync_interval = {existing.config_value}秒")
    else:
        # 插入默认配置
        config = FlinkConfig(
            config_key="checkpoint_sync_interval",
            config_value="30",
            description="Checkpoint同步间隔（秒），系统每N秒同步一次checkpoint状态到数据库"
        )
        session.add(config)
        session.commit()
        print("✅ 已添加默认配置: checkpoint_sync_interval = 30秒")

    # 列出所有Flink配置
    print("\n当前所有Flink配置:")
    all_configs = session.query(FlinkConfig).all()
    for cfg in all_configs:
        print(f"  - {cfg.config_key}: {cfg.config_value} ({cfg.description})")

finally:
    session.close()
