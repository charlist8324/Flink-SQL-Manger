from pathlib import Path

# Flink JobManager REST 地址
FLINK_REST_URL = "http://192.168.31.251:8081"

# Flink SQL Gateway 地址（默认端口 8083）
# 如果 SQL Gateway 和 Flink REST API 不在同一主机，请修改此处
SQL_GATEWAY_URL = "http://192.168.31.251:8083"

# SQL 脚本存放目录（相对后端运行目录）
BASE_DIR = Path(__file__).resolve().parent
SQL_FILES_DIR = BASE_DIR / "sql_jobs"

# 这里填写你预先上传或部署好的 SQL Runner Jar 的 jarId
# 例如: "flink-sql-runner-1.0.jar" 或 REST 上传后返回的 ID
# 如何获取 jarId:
# 1. 访问 http://localhost:8000/docs 或 http://localhost:8000/api/jars 查看已上传的 Jar 列表
# 2. 复制你想使用的 Jar 的 id 字段（类似：12345678-1234-1234-1234-1234567890ab_jar_name.jar）
# 3. 将其粘贴到下面
SQL_RUNNER_JAR_ID = ""  # 留空则只能使用 Jar 方式提交作业

SQL_FILES_DIR.mkdir(exist_ok=True)
