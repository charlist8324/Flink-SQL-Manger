# 修复说明：作业列表和历史作业问题

## 问题 1：实时作业只显示 RUNNING 状态，其他状态在历史作业中显示

### 问题描述
- 实时作业页面（/api/jobs）显示了所有状态的作业，包括 FINISHED、FAILED、CANCELED 等
- 历史作业页面（/api/jobs/history）也显示了所有状态的作业
- 两个页面的数据重复，不符合使用预期

### 修复方案

#### 1. 实时作业接口（/api/jobs）
**修改位置**：`backend/main.py` 第 305-361 行

**主要修改**：
- 添加状态过滤，只返回 RUNNING 状态的作业
- 修复排序逻辑，处理 None 值导致的类型错误

**修改前**：
```python
@app.get("/api/jobs", tags=["作业管理"])
async def get_jobs():
    """获取所有作业列表（优化版）"""
    # 从Flink获取作业列表
    jobs_overview = await flink_client.get_jobs_overview()
    flink_jobs = jobs_overview.get("jobs", [])
    
    # 从数据库获取所有作业（包括已完成的）
    db_jobs = {job["job_id"]: job for job in get_all_jobs(state=None)}
    
    # 转换字段名以兼容前端
    converted_jobs = []
    for flink_job in flink_jobs:
        job_id = flink_job.get("id")
        state = flink_job.get("status")
        # ... 没有过滤状态，所有作业都被添加到列表
        
    # 按start_time降序排序
    converted_jobs.sort(key=lambda x: x.get("start-time", 0), reverse=True)
```

**修改后**：
```python
@app.get("/api/jobs", tags=["作业管理"])
async def get_jobs():
    """获取实时作业列表（只显示RUNNING状态的作业）"""
    # 从Flink获取作业列表
    jobs_overview = await flink_client.get_jobs_overview()
    flink_jobs = jobs_overview.get("jobs", [])
    
    # 从数据库获取所有作业（包括已完成的）
    db_jobs = {job["job_id"]: job for job in get_all_jobs(state=None)}
    
    # 转换字段名以兼容前端
    converted_jobs = []
    for flink_job in flink_jobs:
        job_id = flink_job.get("id")
        state = flink_job.get("status")
        
        # 只处理 RUNNING 状态的作业（实时作业）
        if state != "RUNNING":
            continue
        
        # ... 只添加 RUNNING 状态的作业
        
    # 按start_time降序排序，使用安全的比较方式，避免 None 值导致错误
    converted_jobs.sort(key=lambda x: (x.get("start-time") or 0), reverse=True)
```

#### 2. 历史作业接口（/api/jobs/history）
**修改位置**：`backend/main.py` 第 174-189 行

**主要修改**：
- 添加状态过滤，排除 RUNNING 状态的作业
- 修复排序逻辑，处理 None 值

**修改前**：
```python
@app.get("/api/jobs/history", tags=["作业管理"])
async def get_job_history(limit: int = 50):
    """获取历史作业（从数据库查询）"""
    # 从数据库获取所有作业（包括已完成的）
    all_jobs = get_all_jobs(state=None, limit=limit)
    
    # 按创建时间降序排序
    sorted_jobs = sorted(all_jobs, key=lambda x: x.get("created_at", 0), reverse=True)
    
    return sorted_jobs
```

**修改后**：
```python
@app.get("/api/jobs/history", tags=["作业管理"])
async def get_job_history(limit: int = 50):
    """获取历史作业（从数据库查询，排除RUNNING状态的作业）"""
    # 从数据库获取所有作业
    all_jobs = get_all_jobs(state=None, limit=limit)
    
    # 过滤掉 RUNNING 状态的作业（实时作业），只保留历史作业
    history_jobs = [job for job in all_jobs if job.get("state") != "RUNNING"]
    
    # 按创建时间降序排序，处理 None 值
    sorted_jobs = sorted(
        history_jobs,
        key=lambda x: (x.get("created_at") or 0 if x.get("created_at") is not None else 0),
        reverse=True
    )
    
    return sorted_jobs
```

## 问题 2：恢复作业报错 - 类型比较错误

### 问题描述
**错误信息**：
```
获取作业列表失败: {"detail":"'<' not supported between instances of 'int' and 'NoneType'"}
```

**根本原因**：
在排序作业列表时，某些作业的 `start_time` 或 `created_at` 字段为 `None`，而排序函数尝试比较 `None` 和 `int` 值，导致类型错误。

### 修复方案

#### 修复排序逻辑

**修改前**：
```python
# 不安全的比较方式，如果 start-time 是 None 会报错
converted_jobs.sort(key=lambda x: x.get("start-time", 0), reverse=True)
```

**修改后**：
```python
# 安全的比较方式，使用 or 0 处理 None 值
converted_jobs.sort(key=lambda x: (x.get("start-time") or 0), reverse=True)
```

**修改位置**：
- `backend/main.py` 第 352 行（实时作业排序）
- `backend/main.py` 第 181 行（历史作业排序）

#### 同时修复的数据问题

在获取作业信息时，对可能为 `None` 的字段也进行了处理：

**修改前**：
```python
if db_job:
    job_name = db_job.get("job_name")
    start_time = db_job.get("start_time")  # 可能是 None
    end_time = db_job.get("end_time")      # 可能是 None
    duration = db_job.get("duration")      # 可能是 None
```

**修改后**：
```python
if db_job:
    job_name = db_job.get("job_name")
    start_time = db_job.get("start_time") or 0  # None 值转为 0
    end_time = db_job.get("end_time") or 0      # None 值转为 0
    duration = db_job.get("duration") or 0      # None 值转为 0
```

## 数据流转说明

### 实时作业（/api/jobs）
1. 从 Flink REST API 获取所有作业
2. 从数据库获取所有作业记录
3. **过滤**：只保留 RUNNING 状态的作业
4. 合并 Flink 和数据库的作业信息
5. 按 start_time 降序排序（最新在前）
6. 返回作业列表

### 历史作业（/api/jobs/history）
1. 从数据库获取所有作业记录
2. **过滤**：排除 RUNNING 状态的作业
3. 按 created_at 降序排序（最新在前）
4. 返回作业列表

## 状态说明

### Flink 作业状态
- **RUNNING**：运行中（显示在实时作业）
- **FINISHED**：已完成（显示在历史作业）
- **FAILED**：失败（显示在历史作业）
- **CANCELED**：已取消（显示在历史作业）
- **RESTARTING**：重启中（显示在历史作业）
- **SUSPENDED**：已暂停（显示在历史作业）

### 实时作业页面
- **只显示**：RUNNING 状态的作业
- **不显示**：其他所有状态的作业

### 历史作业页面
- **显示**：FINISHED、FAILED、CANCELED、RESTARTING、SUSPENDED 等状态
- **不显示**：RUNNING 状态的作业

## 测试验证

### 1. 测试状态过滤
运行验证脚本：
```bash
python_portable/python.exe verify_jobs_fix.py
```

预期输出：
- 实时作业：只显示 RUNNING 状态
- 历史作业：显示所有非 RUNNING 状态

### 2. 测试排序功能
- 提交多个作业，检查排序是否正确
- 检查包含 None 值的作业是否正常显示

### 3. 测试恢复作业
- 从历史作业页面恢复作业
- 确认不再出现类型错误

### 4. 测试数据库连接
访问 http://localhost:8000/health，确认数据库连接正常

## 其他修复

同时修复了数据库连接字符串的 URL 编码问题（见 FIX_SUMMARY.md）

## 部署步骤

1. **停止当前服务**（如果正在运行）

2. **重启后端服务**
   ```powershell
   .\setup_and_run.ps1
   ```

3. **验证修复效果**
   - 访问 http://localhost:8000/health 检查数据库连接
   - 查看实时作业页面，应只显示 RUNNING 状态
   - 查看历史作业页面，应只显示非 RUNNING 状态
   - 测试恢复作业功能，确认无错误

## 总结

### 修复的问题
1. ✅ 实时作业只显示 RUNNING 状态
2. ✅ 历史作业只显示非 RUNNING 状态
3. ✅ 修复排序时的类型错误
4. ✅ 修复数据库连接 URL 编码问题

### 改进点
- 状态过滤逻辑清晰
- 排序更安全，处理 None 值
- 日志输出更详细
- 数据流转更合理

### 注意事项
- 重启服务后，之前的历史作业可能会被重新分类
- 某些作业在数据库中可能没有完整信息，会使用 Flink API 的数据
- 建议定期检查数据库中的作业数据完整性
