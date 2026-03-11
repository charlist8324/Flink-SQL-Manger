# 修复说明：作业提交和显示逻辑调整

## 需求说明

### 1. 作业提交逻辑
- 作业通过可视化配置运行后，**立即写入数据库**
- 即使作业还在启动中（没有返回 jobid），也要写入数据库

### 2. 实时作业显示逻辑
- **从 Flink 接口获取**所有作业
- 只显示 **RUNNING 状态**的作业
- 合并数据库中的作业名称和时间信息（优先）

### 3. 历史作业显示逻辑
- **从数据库获取**所有作业
- 只显示 **非 RUNNING 状态**的作业
- 排除 FINISHED、FAILED、CANCELED 等状态

## 修改内容

### 1. 作业提交逻辑（submit_sql_job）

#### 修改位置
`backend/main.py` 第 898-1022 行

#### 修改前
```python
if result.get("jobid"):
    # 只有获取到 jobid 才保存到数据库
    job_id = result["jobid"]
    save_job(...)
    update_job_state(job_id, "RUNNING", ...)
elif result.get("operationHandle"):
    # 作业正在启动中，不保存到数据库
    return {"flink_job_id": None, ...}
```

#### 修改后
```python
if result.get("jobid"):
    # 立即保存作业信息到数据库
    job_id = result["jobid"]
    save_job(...)  # 立即写入数据库
    update_job_state(job_id, "RUNNING", ...)
    return JobSubmitResponse(...)

elif result.get("operationHandle"):
    # 作业正在启动中，也保存到数据库
    operation_handle = result.get("operationHandle")
    
    # 生成临时作业 ID（使用操作句柄）
    temp_job_id = f"temp_{operation_handle[:32]}"
    
    # 立即保存作业信息到数据库
    save_job(
        job_id=temp_job_id,
        job_name=req.job_name,
        sql_text=req.sql_text,
        parallelism=req.parallelism
    )
    
    # 记录操作日志
    log_operation(...)
    
    return {
        "job_name": req.job_name,
        "flink_job_id": None,
        "operation_handle": operation_handle,
        "temp_job_id": temp_job_id,
        "message": "作业已提交，正在启动中",
        "status": "success"
    }
```

#### 关键改进
- ✅ 作业提交后**立即写入数据库**，不等待作业启动完成
- ✅ 作业在启动中时，使用临时 ID 保存到数据库
- ✅ 确保提交的作业能立即在历史作业中看到

### 2. 实时作业显示逻辑（get_jobs）

#### 修改位置
`backend/main.py` 第 311-366 行

#### 修改后的逻辑
```python
@app.get("/api/jobs", tags=["作业管理"])
async def get_jobs():
    """获取实时作业列表（从Flink接口获取，只显示RUNNING状态的作业）"""
    try:
        # 从 Flink 获取作业列表
        jobs_overview = await flink_client.get_jobs_overview()
        flink_jobs = jobs_overview.get("jobs", [])
        
        # 从数据库获取所有作业，用于补充作业名称等信息
        db_jobs = {job["job_id"]: job for job in get_all_jobs(state=None)}
        
        # 转换字段名以兼容前端
        converted_jobs = []
        for flink_job in flink_jobs:
            job_id = flink_job.get("id")
            state = flink_job.get("status")
            
            # 只显示 RUNNING 状态的作业（实时作业）
            if state != "RUNNING":
                continue
            
            # 从数据库获取作业信息（优先）
            db_job = db_jobs.get(job_id)
            
            if db_job:
                # 使用数据库中的作业名称和时间信息
                job_name = db_job.get("job_name")
                start_time = db_job.get("start_time") or 0
                end_time = db_job.get("end_time") or 0
                duration = db_job.get("duration") or 0
            else:
                # 降级：使用 Flink API 返回的基本信息
                job_name = flink_job.get("name", job_id)
                start_time = flink_job.get("start-time", 0) or 0
                end_time = flink_job.get("end-time", 0) or 0
                duration = flink_job.get("duration", 0) or 0
            
            converted_job = {
                "jid": job_id,
                "state": state,
                "name": job_name,
                "start-time": start_time,
                "end-time": end_time,
                "duration": duration
            }
            converted_jobs.append(converted_job)

        # 按start_time降序排序（最新的在前面）
        converted_jobs.sort(key=lambda x: (x.get("start-time") or 0), reverse=True)
        
        return converted_jobs
    except Exception as e:
        logger.error(f"获取作业列表失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))
```

#### 关键改进
- ✅ 从 **Flink 接口**获取所有作业
- ✅ 只显示 **RUNNING 状态**的作业
- ✅ 优先使用数据库中的作业名称和时间信息
- ✅ 如果数据库没有该作业，使用 Flink API 的信息

### 3. 历史作业显示逻辑（get_job_history）

#### 修改位置
`backend/main.py` 第 174-196 行

#### 修改后的逻辑（无需修改，已经是正确的）
```python
@app.get("/api/jobs/history", tags=["作业管理"])
async def get_job_history(limit: int = 50):
    """获取历史作业（从数据库查询，排除RUNNING状态的作业）"""
    try:
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
        
        logger.info(f"返回 {len(sorted_jobs)} 个历史作业（从 {len(all_jobs)} 个作业中过滤）")
        return sorted_jobs
    except Exception as e:
        logger.error(f"Failed to get history jobs: {e}")
        raise HTTPException(status_code=500, detail=str(e))
```

#### 关键改进
- ✅ 从 **数据库**获取所有作业
- ✅ 只显示 **非 RUNNING 状态**的作业
- ✅ 按创建时间降序排序

## 数据流转说明

### 作业提交流程

```
用户提交作业
    ↓
提交到 Flink SQL Gateway
    ↓
获取 jobid 或 operation_handle
    ↓
【立即写入数据库】
    ├─ 有 jobid：使用 jobid 保存
    └─ 无 jobid（正在启动）：使用临时 ID 保存
    ↓
返回结果给前端
```

### 实时作业查询流程

```
前端请求 /api/jobs
    ↓
从 Flink API 获取所有作业
    ↓
过滤：只保留 RUNNING 状态
    ↓
合并数据库信息（作业名称、时间）
    ↓
按 start_time 降序排序
    ↓
返回作业列表
```

### 历史作业查询流程

```
前端请求 /api/jobs/history
    ↓
从数据库获取所有作业
    ↓
过滤：排除 RUNNING 状态
    ↓
按 created_at 降序排序
    ↓
返回作业列表
```

## 状态说明

### Flink 作业状态
| 状态 | 显示位置 | 说明 |
|------|---------|------|
| RUNNING | 实时作业 | 运行中，从 Flink API 获取 |
| FINISHED | 历史作业 | 已完成 |
| FAILED | 历史作业 | 失败 |
| CANCELED | 历史作业 | 已取消 |
| RESTARTING | 历史作业 | 重启中 |
| SUSPENDED | 历史作业 | 已暂停 |

### 数据库作业状态
| 状态 | 说明 |
|------|------|
| CREATED | 作业已创建 |
| RUNNING | 作业运行中 |
| FINISHED | 作业已完成 |
| FAILED | 作业失败 |
| CANCELED | 作业已取消 |
| STOPPED | 作业已停止 |
| RESUMED | 作业已恢复 |

## 部署步骤

### 1. 停止当前服务
如果后端服务正在运行，先停止它。

### 2. 重启后端服务
```powershell
.\setup_and_run.ps1
```

### 3. 验证修复效果

#### 测试作业提交
1. 提交一个新的作业
2. 立即查看数据库 `flink_jobs` 表
3. 确认作业已立即写入数据库

#### 测试实时作业
1. 访问 http://localhost:8000/api/jobs
2. 确认只显示 RUNNING 状态的作业
3. 确认作业名称来自数据库（如果有）

#### 测试历史作业
1. 访问 http://localhost:8000/api/jobs/history
2. 确认只显示非 RUNNING 状态的作业
3. 确认刚刚提交的作业（如果是启动中）也在历史作业中

### 4. 检查日志
```bash
tail -f logs/run.log
```

## 已修复的问题

### 问题 1：作业提交后未立即写入数据库 ✅
**原因**：只有获取到 jobid 才保存到数据库
**修复**：作业提交后立即写入数据库，即使作业还在启动中

### 问题 2：实时作业显示所有状态 ✅
**原因**：未过滤作业状态
**修复**：只显示 RUNNING 状态的作业

### 问题 3：历史作业包含 RUNNING 状态 ✅
**原因**：未过滤作业状态
**修复**：排除 RUNNING 状态的作业

### 问题 4：排序类型错误 ✅
**原因**：比较 None 和 int 值
**修复**：使用 `or 0` 处理 None 值

### 问题 5：数据库连接失败 ✅
**原因**：密码中的 @ 符号未被 URL 编码
**修复**：添加 URL 编码处理

## 注意事项

1. **临时 ID**：作业在启动中使用临时 ID（`temp_{operation_handle[:32]}`），后续需要更新为真实的 jobid
2. **数据同步**：实时作业从 Flink API 获取，历史作业从数据库获取，两者数据可能存在短暂延迟
3. **日志检查**：定期检查日志，及时发现异常
4. **数据库清理**：定期清理无效的历史作业记录

## 相关文件

- **主要修改**：`backend/main.py`
  - `submit_sql_job()` 函数（第 898-1022 行）
  - `get_jobs()` 函数（第 311-366 行）
  - `get_job_history()` 函数（第 174-196 行）

- **数据库配置**：`backend/database.py`
  - `get_database_url()` 函数（添加了 URL 编码）

## 总结

本次修复确保了：
1. ✅ 作业提交后立即写入数据库
2. ✅ 实时作业从 Flink 接口获取，只显示 RUNNING 状态
3. ✅ 历史作业从数据库获取，只显示非 RUNNING 状态
4. ✅ 修复了数据库连接问题
5. ✅ 修复了排序类型错误

所有修改已完成，重启服务后即可生效！
