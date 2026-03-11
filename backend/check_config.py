from db_operations import get_flink_config

value = get_flink_config('checkpoint_sync_interval')
print(f'checkpoint_sync_interval = {value}')
