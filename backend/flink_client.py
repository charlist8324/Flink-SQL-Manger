"""
Flink REST API 客户端
封装与 Flink JobManager 的所有交互
"""
import logging
import asyncio
import time
import httpx
from typing import Optional, List, Dict, Any
from config import FLINK_REST_URL

logger = logging.getLogger(__name__)


class FlinkClient:
    """Flink REST API 客户端"""

    def __init__(self, base_url: str = FLINK_REST_URL, sql_gateway_url: str = None):
        from config import SQL_GATEWAY_URL
        self.base_url = base_url.rstrip("/")
        self.sql_gateway_url = (sql_gateway_url or SQL_GATEWAY_URL).rstrip("/")

    async def _request(self, method: str, path: str, use_sql_gateway: bool = False, **kwargs) -> Dict[str, Any]:
        """发送 HTTP 请求"""
        base = self.sql_gateway_url if use_sql_gateway else self.base_url
        url = f"{base}{path}"
        
        logger.debug(f"发送 HTTP 请求: {method} {url}")
        logger.debug(f"参数: {kwargs}")
        
        # INSERT 语句需要更长的超时时间，因为作业提交需要时间
        timeout = kwargs.pop('timeout', None) or (httpx.Timeout(300.0) if use_sql_gateway and method == 'POST' and 'statements' in path else 30.0)
        
        async with httpx.AsyncClient(timeout=timeout) as client:
            try:
                resp = await client.request(method, url, **kwargs)
                
                if resp.status_code >= 400:
                    error_text = resp.text
                    logger.error(f"HTTP 错误: {resp.status_code} - {error_text}")
                    raise Exception(f"Flink API error: {resp.status_code} - {error_text}")
                
                logger.debug(f"响应状态码: {resp.status_code}")
                result = resp.json() if resp.text else {}
                logger.debug(f"响应结果（前 500 字符）: {str(result)[:500]}")
                return result
                
            except Exception as e:
                logger.error(f"HTTP 请求失败: {e}")
                raise

    # ============ 集群信息 ============
    async def get_cluster_overview(self) -> Dict[str, Any]:
        """获取集群概览"""
        return await self._request("GET", "/overview")

    async def get_cluster_config(self) -> Dict[str, Any]:
        """获取集群配置"""
        return await self._request("GET", "/config")

    async def get_taskmanagers(self) -> List[Dict[str, Any]]:
        """获取所有 TaskManager"""
        data = await self._request("GET", "/taskmanagers")
        return data.get("taskmanagers", [])

    async def get_taskmanager_detail(self, tm_id: str) -> Dict[str, Any]:
        """获取 TaskManager 详情"""
        return await self._request("GET", f"/taskmanagers/{tm_id}")

    async def get_taskmanager_metrics(self, tm_id: str, metrics: List[str] = None) -> List[Dict[str, Any]]:
        """获取 TaskManager 指标"""
        params = {}
        if metrics:
            params["get"] = ",".join(metrics)
        return await self._request("GET", f"/taskmanagers/{tm_id}/metrics", params=params)

    # ============ 作业管理 ============
    async def get_jobs_overview(self) -> Dict[str, Any]:
        """获取所有作业概览"""
        # Flink 1.8 使用 /jobs 而不是 /jobs/overview
        data = await self._request("GET", "/jobs")
        # 转换为 overview 格式
        jobs_list = data.get("jobs", []) if isinstance(data.get("jobs"), list) else data.get("jobs-running", []) + data.get("jobs-finished", []) + data.get("jobs-cancelled", []) + data.get("jobs-failed", [])
        return {"jobs": jobs_list}

    async def get_jobs(self) -> List[Dict[str, Any]]:
        """获取所有作业列表"""
        data = await self._request("GET", "/jobs")
        # Flink 1.8 返回格式: {"jobs-running": [], "jobs-finished": [], ...}
        if "jobs" in data and isinstance(data["jobs"], list):
            return data["jobs"]
        # 兼容 1.8 格式
        all_jobs = []
        for key in ["jobs-running", "jobs-finished", "jobs-cancelled", "jobs-failed"]:
            if key in data:
                all_jobs.extend(data[key])
        return all_jobs

    async def get_job_detail(self, job_id: str) -> Dict[str, Any]:
        """获取作业详情"""
        return await self._request("GET", f"/jobs/{job_id}")

    async def get_job_plan(self, job_id: str) -> Dict[str, Any]:
        """获取作业执行计划"""
        return await self._request("GET", f"/jobs/{job_id}/plan")

    async def get_job_exceptions(self, job_id: str) -> Dict[str, Any]:
        """获取作业异常信息"""
        return await self._request("GET", f"/jobs/{job_id}/exceptions")

    async def get_job_config(self, job_id: str) -> Dict[str, Any]:
        """获取作业配置"""
        return await self._request("GET", f"/jobs/{job_id}/config")

    async def cancel_job(self, job_id: str) -> Dict[str, Any]:
        """取消作业"""
        logger.debug(f"尝试取消作业: {job_id}")
        # Flink 1.8 使用 POST 方法
        try:
            result = await self._request("POST", f"/jobs/{job_id}/cancel")
            logger.info(f"使用 POST /jobs/{job_id}/cancel 取消作业成功")
            return result
        except Exception as e:
            logger.warning(f"POST /jobs/{job_id}/cancel 失败: {e}, 尝试使用 PATCH 方法")
            # 如果 POST 失败，尝试 PATCH
            result = await self._request("PATCH", f"/jobs/{job_id}", params={"mode": "cancel"})
            logger.info(f"使用 PATCH /jobs/{job_id}?mode=cancel 取消作业成功")
            return result

    async def stop_job(self, job_id: str) -> Dict[str, Any]:
        """暂停作业（停止运行）"""
        logger.debug(f"尝试停止作业: {job_id}")
        try:
            result = await self._request("POST", f"/jobs/{job_id}/stop")
            logger.info(f"使用 POST /jobs/{job_id}/stop 停止作业成功")
            return result
        except Exception as e:
            logger.warning(f"POST /jobs/{job_id}/stop 失败: {e}, 尝试使用 PATCH 方法")
            # 如果 POST 失败，尝试 PATCH
            result = await self._request("PATCH", f"/jobs/{job_id}", params={"mode": "stop"})
            logger.info(f"使用 PATCH /jobs/{job_id}?mode=stop 停止作业成功")
            return result

    async def restart_job(self, job_id: str, savepoint_path: str = None) -> Dict[str, Any]:
        """重启作业"""
        body = {}
        if savepoint_path:
            body["from-savepoint"] = savepoint_path
        try:
            return await self._request("PATCH", f"/jobs/{job_id}", json=body)
        except:
            # 如果 PATCH 失败，尝试 POST
            return await self._request("POST", f"/jobs/{job_id}/restart", json=body)

    async def stop_job_with_savepoint(self, job_id: str, target_directory: str = None) -> Dict[str, Any]:
        """停止作业并创建 savepoint"""
        # 根据Flink文档，使用POST /jobs/{job_id}/stop API
        # 传递targetDirectory参数来创建savepoint并停止作业
        body = {}
        if target_directory:
            body["targetDirectory"] = target_directory
        
        logger.info(f"调用Flink API停止作业 {job_id} 并创建savepoint，目标目录: {target_directory}")
        try:
            result = await self._request("POST", f"/jobs/{job_id}/stop", json=body)
            logger.info(f"Stop job API response: {result}")
        except Exception as e:
            logger.error(f"Stop job API call failed: {e}")
            raise

        # 检查是否返回了 trigger id (request-id 或 trigger-id)
        request_id = result.get("request-id") or result.get("trigger-id")
        
        if request_id:
            logger.info(f"Stop trigger ID received: {request_id}, polling for completion...")
            
            # 轮询等待完成 (增加超时时间到 60 秒)
            for i in range(60): 
                await asyncio.sleep(1)
                try:
                    # 检查 savepoint 状态
                    # Flink 1.15+ 使用 /jobs/:jobid/savepoints/:triggerid 查询
                    status_resp = await self._request("GET", f"/jobs/{job_id}/savepoints/{request_id}")
                    status_info = status_resp.get("status", {})
                    status = status_info.get("id")
                    
                    if status == "COMPLETED":
                        operation = status_resp.get("operation", {})
                        location = operation.get("location")
                        logger.info(f"✅ Stop with savepoint completed. Response: {status_resp}")
                        
                        if not location:
                            logger.warning(f"⚠️ Savepoint completed but location is missing! Operation details: {operation}")
                            # 尝试查找可能的字段
                            location = operation.get("path") or operation.get("target-directory")
                        
                        # 尝试从 history 获取更详细的信息（如 timestamp）
                        savepoint_timestamp = int(time.time() * 1000)
                        savepoint_id = None
                        
                        try:
                            # 等待一下，确保 history server 同步
                            await asyncio.sleep(2)
                            savepoints_result = await self.get_job_savepoints(job_id)
                            savepoints = savepoints_result.get("savepoints", [])
                            if savepoints:
                                # 查找匹配路径的savepoint
                                matched_sp = None
                                for sp in savepoints:
                                    if sp.get("path") == location or sp.get("external_path") == location:
                                        matched_sp = sp
                                        break
                                
                                if matched_sp:
                                    savepoint_timestamp = matched_sp.get("timestamp")
                                    savepoint_id = matched_sp.get("id")
                                    logger.info(f"✅ Found savepoint details in history: id={savepoint_id}, ts={savepoint_timestamp}")
                                else:
                                    # 如果没有精确匹配，但列表非空，取最新的（如果时间接近）
                                    latest = savepoints[0]
                                    logger.info(f"Using latest savepoint from history: {latest.get('path')}")
                                    # 这里可以稍微放宽条件，但为了安全起见，还是只用location
                        except Exception as history_err:
                            logger.warning(f"Could not fetch history after stop: {history_err}")
                            
                        return {
                            "job_id": job_id,
                            "savepoint_path": location,
                            "savepoint_id": savepoint_id,
                            "savepoint_timestamp": savepoint_timestamp,
                            "status": "COMPLETED"
                        }
                    elif status == "IN_PROGRESS":
                        if i % 5 == 0:
                            logger.debug(f"Savepoint still in progress... ({i}s)")
                        continue
                    elif status == "FAILED":
                        failure_cause = status_resp.get("operation", {}).get("failure-cause", {})
                        error_msg = failure_cause.get("stack-trace") or str(failure_cause)
                        logger.error(f"❌ Stop with savepoint failed: {error_msg}")
                        break
                    else:
                        logger.warning(f"Stop failed with status: {status}")
                        break
                except Exception as poll_err:
                    logger.warning(f"Polling stop status failed: {poll_err}")
                    # 如果是 404，可能是 job 已经彻底消失或者 trigger id 过期
                    if "404" in str(poll_err):
                        logger.warning("Trigger ID or Job not found during polling (404).")
                    # 继续尝试 polling，直到超时，除非确定不可恢复
                    if i > 10 and "404" in str(poll_err):
                         break
                    continue

        else:
            logger.warning("No request-id returned from stop job call. Falling back to delay.")

        # Fallback: 如果 polling 失败或没有 request-id，尝试直接从 checkpoints/history 获取
        logger.info("Polling failed or incomplete. Attempting to fetch latest savepoint/checkpoint...")
        await asyncio.sleep(2)
        
        try:
            # 获取最新的savepoint信息
            savepoints_result = await self.get_job_savepoints(job_id)
            savepoints = savepoints_result.get("savepoints", [])
            if savepoints:
                # 返回最新的savepoint（最新的在前面）
                latest_savepoint = savepoints[0]
                savepoint_path = latest_savepoint.get("path") or latest_savepoint.get("external_path")
                savepoint_timestamp = latest_savepoint.get("timestamp")
                logger.info(f"✅ Retrieved latest savepoint from history (fallback): {savepoint_path}")
                return {
                    "job_id": job_id,
                    "savepoint_path": savepoint_path,
                    "savepoint_id": latest_savepoint.get("id"),
                    "savepoint_timestamp": savepoint_timestamp,
                    "status": "COMPLETED"
                }
            else:
                logger.warning("❌ No savepoints found in history.")
        except Exception as e:
            logger.warning(f"获取savepoint信息失败 (fallback): {e}")
        
        # 如果无法获取savepoint信息，返回原始结果（可能包含 request-id）
        logger.info(f"Returning original stop result: {result}")
        return result

    async def get_job_savepoints(self, job_id: str) -> Dict[str, Any]:
        """获取作业 savepoints 列表"""
        try:
            return await self._request("GET", f"/jobs/{job_id}/savepoints")
        except Exception as e:
            logger.warning(f"GET /jobs/{job_id}/savepoints failed: {e}. Trying checkpoints...")
            try:
                # Flink 1.18 可能没有单独的 savepoints 端点，从 checkpoints 中获取
                checkpoints = await self.get_job_checkpoints(job_id)
                savepoints = []
                if "history" in checkpoints:
                    savepoints = [
                        {
                            "id": cp.get("id"),
                            "path": cp.get("external_path"),
                            "timestamp": cp.get("timestamp"),
                            "status": "COMPLETED"
                        }
                        for cp in checkpoints["history"]
                        if cp.get("external_path")
                    ]
                return {"savepoints": savepoints}
            except Exception as cp_err:
                logger.warning(f"Failed to get checkpoints for {job_id}: {cp_err}")
                return {"savepoints": []}

    # ============ 作业指标 ============
    async def get_job_metrics(self, job_id: str, metrics: List[str] = None) -> Dict[str, Any]:
        """获取作业指标"""
        params = {}
        if metrics:
            params["get"] = ",".join(metrics)
        return await self._request("GET", f"/jobs/{job_id}/metrics", params=params)

    async def get_vertex_metrics(self, job_id: str, vertex_id: str, metrics: List[str] = None) -> Dict[str, Any]:
        """获取算子指标"""
        params = {}
        if metrics:
            params["get"] = ",".join(metrics)
        return await self._request("GET", f"/jobs/{job_id}/vertices/{vertex_id}/metrics", params=params)

    # ============ Checkpoints ============
    async def get_job_checkpoints(self, job_id: str) -> Dict[str, Any]:
        """获取作业 checkpoint 信息"""
        return await self._request("GET", f"/jobs/{job_id}/checkpoints")

    async def trigger_savepoint(self, job_id: str, target_directory: Optional[str] = None, cancel_job: bool = False) -> Dict[str, Any]:
        """触发 savepoint"""
        body = {}
        if target_directory:
            body["target-directory"] = target_directory
        if cancel_job:
            body["cancel-job"] = cancel_job
        return await self._request("POST", f"/jobs/{job_id}/savepoints", json=body)

    # ============ Jar 管理 ============
    async def get_jars(self) -> List[Dict[str, Any]]:
        """获取所有已上传的 Jar"""
        data = await self._request("GET", "/jars")
        return data.get("files", [])

    async def upload_jar(self, jar_content: bytes, filename: str) -> Dict[str, Any]:
        """上传 Jar 文件"""
        async with httpx.AsyncClient() as client:
            files = {"jarfile": (filename, jar_content, "application/x-java-archive")}
            resp = await client.post(f"{self.base_url}/jars/upload", files=files, timeout=60.0)
            if resp.status_code >= 400:
                raise Exception(f"Upload jar failed: {resp.status_code} - {resp.text}")
            return resp.json()

    async def delete_jar(self, jar_id: str) -> Dict[str, Any]:
        """删除 Jar"""
        return await self._request("DELETE", f"/jars/{jar_id}")

    async def run_jar(
        self,
        jar_id: str,
        entry_class: str = None,
        program_args: str = None,
        parallelism: int = None,
        savepoint_path: str = None,
        allow_non_restored_state: bool = False
    ) -> Dict[str, Any]:
        """运行 Jar"""
        body = {}
        if entry_class:
            body["entryClass"] = entry_class
        if program_args:
            body["programArgs"] = program_args
        if parallelism:
            body["parallelism"] = parallelism
        if savepoint_path:
            body["savepointPath"] = savepoint_path
        if allow_non_restored_state:
            body["allowNonRestoredState"] = allow_non_restored_state

        return await self._request("POST", f"/jars/{jar_id}/run", json=body)

    async def get_jar_plan(self, jar_id: str, entry_class: str = None, program_args: str = None) -> Dict[str, Any]:
        """获取 Jar 执行计划（不运行）"""
        params = {}
        if entry_class:
            params["entry-class"] = entry_class
        if program_args:
            params["program-args"] = program_args
        return await self._request("GET", f"/jars/{jar_id}/plan", params=params)

    # ============ SQL Gateway ============
    async def create_session(self, session_name: str = None, job_name: str = None, savepoint_path: str = None) -> Dict[str, Any]:
        """创建 SQL Gateway 会话"""
        body = {}
        if session_name:
            body["sessionName"] = session_name
        # 设置重启策略：失败后重试3次，每次间隔10秒
        # 这样可以避免因临时资源问题导致的作业失败
        properties = {
            "restart-strategy": "fixed-delay",
            "restart-strategy.fixed-delay.attempts": "3",
            "restart-strategy.fixed-delay.delay": "10s"
        }
        if job_name:
            # 在会话级别设置作业名称
            properties["table.exec.job.name"] = job_name
            logger.info(f"✅ 设置作业名称: {job_name}")
        if savepoint_path:
            # 在会话级别设置savepoint路径（关键！）
            properties["execution.savepoint.path"] = savepoint_path
            logger.info(f"✅ 设置savepoint路径: {savepoint_path}")
        body["properties"] = properties
        return await self._request("POST", "/v1/sessions", json=body, use_sql_gateway=True)

    async def get_session(self, session_handle: str) -> Dict[str, Any]:
        """获取会话信息"""
        return await self._request("GET", f"/v1/sessions/{session_handle}", use_sql_gateway=True)

    async def execute_sql(self, session_handle: str, sql: str, job_name: Optional[str] = None, savepoint_path: Optional[str] = None) -> Dict[str, Any]:
        """执行 SQL 语句"""
        body = {"statement": sql}
        execution_config = {}
        
        # Flink 1.18.1 使用 executionConfig 而不是 configuration
        # 注意：savepoint_path现在在会话级别设置，不需要在这里重复设置
        if job_name:
            execution_config["table.exec.job.name"] = job_name
            logger.info(f"✅ 执行SQL时设置作业名称: {job_name}")
        
        if execution_config:
            body["executionConfig"] = execution_config
            logger.info(f"✅ 执行SQL时executionConfig: {execution_config}")
        
        return await self._request("POST", f"/v1/sessions/{session_handle}/statements", json=body, use_sql_gateway=True)

    async def get_operation_status(self, session_handle: str, operation_handle: str) -> Dict[str, Any]:
        """获取操作状态"""
        return await self._request("GET", f"/v1/sessions/{session_handle}/operations/{operation_handle}/status", use_sql_gateway=True)

    async def get_operation_result(self, session_handle: str, operation_handle: str, token: str = None) -> Dict[str, Any]:
        """获取操作结果"""
        params = {}
        if token:
            params["token"] = token
        return await self._request("GET", f"/v1/sessions/{session_handle}/operations/{operation_handle}/result", params=params, use_sql_gateway=True)

    async def close_session(self, session_handle: str) -> Dict[str, Any]:
        """关闭会话"""
        return await self._request("DELETE", f"/v1/sessions/{session_handle}", use_sql_gateway=True)

    async def submit_sql_job(self, sql: str, job_name: str = None, parallelism: int = 1, savepoint_path: Optional[str] = None) -> Dict[str, Any]:
        """
        直接提交 SQL 作业（通过 SQL Gateway）
        返回作业 ID
        支持多个 SQL 语句，按顺序执行
        """
        # 创建会话，在会话级别设置作业名称和savepoint路径
        logger.info(f"创建 SQL Gateway 会话，作业名称: {job_name or 'flink-manager-session'}")
        if savepoint_path:
            logger.info(f"✅ Savepoint路径: {savepoint_path}")
        session = await self.create_session(
            session_name=job_name or "flink-manager-session", 
            job_name=job_name,
            savepoint_path=savepoint_path
        )
        session_handle = session.get("sessionHandle")
        logger.info(f"会话句柄: {session_handle}")

        insert_job_id = None
        insert_operation_handle = None

        try:
            # 分割 SQL 语句（按分号分割，忽略空行和注释）
            statements = []
            current_statement = ""
            
            for line in sql.split('\n'):
                # 跳过注释和空行
                stripped = line.strip()
                if not stripped or stripped.startswith('--'):
                    continue
                current_statement += line + '\n'
                # 如果行以分号结尾，说明语句结束
                if stripped.endswith(';'):
                    statements.append(current_statement.strip())
                    current_statement = ""
            
            # 如果还有未完成的语句，添加到列表
            if current_statement.strip():
                statements.append(current_statement.strip())
            
            logger.info(f"共检测到 {len(statements)} 个 SQL 语句")
            
            # 逐个执行 SQL 语句
            for i, stmt in enumerate(statements):
                logger.info(f"执行第 {i+1}/{len(statements)} 条 SQL 语句: {stmt[:100]}...")
                
                # 检查是否是 INSERT 语句
                is_insert = stmt.strip().upper().startswith("INSERT")
                
                # 执行SQL，始终传递job_name
                result = await self.execute_sql(
                    session_handle, 
                    stmt, 
                    job_name=job_name
                    # 注意：savepoint_path已在会话级别设置，不需要在这里重复设置
                )
                logger.info(f"执行结果: {result}")

                # 检查是否是 INSERT 语句
                if is_insert:
                    operation_handle = result.get("operationHandle")
                    job_ids = result.get("jobIds", [])

                    # 如果是 INSERT 语句并且有 jobIds
                    if job_ids:
                        insert_job_id = job_ids[0]
                        logger.info(f"✅ INSERT 语句，作业 ID: {insert_job_id}")
                    else:
                        # 作业还在启动中，保存 operation_handle
                        insert_operation_handle = operation_handle
                        logger.info(f"⏳ INSERT 语句已提交，作业正在启动中，操作句柄: {operation_handle}")

            # 返回 INSERT 语句的作业 ID 或操作句柄
            if insert_job_id:
                logger.info(f"✅ 所有 SQL 语句执行成功，作业 ID: {insert_job_id}")
                # 延迟关闭 Session，避免中断作业提交
                try:
                    await asyncio.sleep(0.5)
                    await self.close_session(session_handle)
                    logger.info(f"✅ Session 已关闭")
                except Exception as e:
                    logger.warning(f"关闭会话失败（可忽略）: {e}")
                return {"jobid": insert_job_id, "status": "success"}
            elif insert_operation_handle:
                logger.info(f"✅ INSERT 语句已提交，作业正在启动中，操作句柄: {insert_operation_handle}")
                # 轮询获取作业 ID - 快速响应
                max_retries = 5  # 减少重试次数
                retry_delay = 0.3  # 减少等待时间

                for retry in range(max_retries):
                    # 第一次不等待，后续每次等待
                    if retry > 0:
                        await asyncio.sleep(retry_delay)
                    
                    # 方法1：尝试从operation result获取jobid
                    try:
                        result_response = await self._request(
                            "GET",
                            f"/v1/sessions/{session_handle}/operations/{insert_operation_handle}/result",
                            use_sql_gateway=True
                        )
                        logger.info(f"第 {retry + 1} 次查询operation result: {result_response}")
                        
                        # 检查result中是否有jobid
                        job_ids = result_response.get("jobIds", [])
                        if job_ids:
                            job_id = job_ids[0]
                            logger.info(f"✅ 从operation result获取到作业 ID: {job_id}")
                            try:
                                await self.close_session(session_handle)
                                logger.info(f"✅ Session 已关闭")
                            except Exception as e2:
                                logger.warning(f"关闭会话失败（可忽略）: {e2}")
                            return {"jobid": job_id, "status": "success"}
                    except Exception as e:
                        # SQL Gateway operation 可能已过期，这是正常的
                        logger.debug(f"第 {retry + 1} 次获取operation result失败 (可能已过期): {e}")
                    
                    # 方法2：直接从 Flink REST API 查询作业列表
                    try:
                        jobs = await self.get_jobs()
                        if jobs and len(jobs) > 0:
                            latest_job = jobs[0]
                            state = latest_job.get("state")
                            job_id = latest_job.get("jid")
                            # 放宽状态检查条件，包括所有可能的状态
                            if state in ["RUNNING", "INITIALIZING", "RESTARTING", "CREATED", "SCHEDULED"]:
                                logger.info(f"✅ 通过 Flink REST API 获取到作业 ID: {job_id} (状态: {state})")
                                try:
                                    await self.close_session(session_handle)
                                    logger.info(f"✅ Session 已关闭")
                                except Exception as e2:
                                    logger.warning(f"关闭会话失败（可忽略）: {e2}")
                                return {"jobid": job_id, "status": "success"}
                            elif retry == max_retries - 1 and job_id:
                                # 最后一次重试，即使不是RUNNING也返回
                                logger.info(f"✅ 最后一次重试，返回作业 ID: {job_id} (状态: {state})")
                                try:
                                    await self.close_session(session_handle)
                                    logger.info(f"✅ Session 已关闭")
                                except Exception as e2:
                                    logger.warning(f"关闭会话失败（可忽略）: {e2}")
                                return {"jobid": job_id, "status": "success"}
                            else:
                                logger.debug(f"第 {retry + 1} 次轮询: 最新作业状态为 {state}，继续等待...")
                        else:
                            logger.debug(f"第 {retry + 1} 次轮询: Flink 中没有作业，继续等待...")
                    except Exception as e:
                        logger.warning(f"第 {retry + 1} 次获取作业列表失败: {e}")

                # 无法获取作业 ID，返回操作句柄和running状态
                logger.warning(f"⚠️ 无法获取作业 ID，返回操作句柄")
                try:
                    await self.close_session(session_handle)
                except Exception as e:
                    logger.warning(f"关闭会话失败（可忽略）: {e}")
                
                return {
                    "jobid": None, 
                    "operationHandle": insert_operation_handle,
                    "status": "running",
                    "message": "INSERT 语句已提交，作业正在启动中"
                }
            else:
                logger.warning(f"⚠️ 未找到 INSERT 语句")
                try:
                    await self.close_session(session_handle)
                except Exception as e:
                    logger.error(f"关闭会话失败: {e}")
                return {
                    "jobid": None,
                    "status": "no_insert",
                    "message": "SQL 已成功执行，但没有 INSERT 语句"
                }

        except Exception as e:
            logger.error(f"提交 SQL 作业时发生异常: {e}")
            import traceback
            logger.error(f"异常详情: {traceback.format_exc()}")
            # 尝试关闭 Session
            try:
                await self.close_session(session_handle)
            except Exception as e2:
                logger.warning(f"关闭会话失败（可忽略）: {e2}")
            raise

    # ============ 健康检查 ============
    async def check_health(self) -> bool:
        """检查 Flink 集群是否可用"""
        try:
            await self._request("GET", "/overview")
            return True
        except:
            return False

 
# 默认客户端实例
flink_client = FlinkClient()
