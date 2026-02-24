const { ref, reactive, computed, onMounted, onUnmounted } = Vue;
const { ElMessage, ElMessageBox } = ElementPlus;

function formatTime(timestamp) {
    if (!timestamp) return '-';
    const date = new Date(timestamp);
    return date.toLocaleString('zh-CN');
}

function formatDuration(ms) {
    if (!ms) return '-';
    const seconds = Math.floor(ms / 1000);
    const minutes = Math.floor(seconds / 60);
    const hours = Math.floor(minutes / 60);
    const days = Math.floor(hours / 24);
    
    if (days > 0) return `${days}天${hours % 24}时${minutes % 60}分`;
    if (hours > 0) return `${hours}时${minutes % 60}分${seconds % 60}秒`;
    if (minutes > 0) return `${minutes}分${seconds % 60}秒`;
    return `${seconds}秒`;
}

function formatTimestamp(ts) {
    if (!ts) return '-';
    return new Date(ts).toLocaleString('zh-CN');
}

function formatJobState(state) {
    const stateMap = {
        'RUNNING': '运行中',
        'FINISHED': '已完成',
        'FAILED': '失败',
        'CANCELED': '已取消',
        'STOPPED': '已暂停',
        'CREATED': '已创建',
        'RESTARTING': '重启中',
        'SCHEDULED': '调度中',
        'RECONCILING': '恢复中',
        'RESUMED': '已恢复'
    };
    return stateMap[state] || state;
}

function getJobStateType(state) {
    const typeMap = {
        'RUNNING': 'success',
        'FINISHED': 'info',
        'FAILED': 'danger',
        'CANCELED': 'warning',
        'STOPPED': 'warning',
        'CREATED': '',
        'RESTARTING': 'warning',
        'SCHEDULED': '',
        'RECONCILING': 'warning',
        'RESUMED': 'success'
    };
    return typeMap[state] || 'info';
}

export default {
    setup() {
        const jobTab = ref('running');
        const jobs = ref([]);
        const loadingJobs = ref(false);
        const historyJobs = ref([]);
        const loadingHistoryJobs = ref(false);
        const historyStateFilter = ref('');
        
        const filteredHistoryJobs = computed(() => {
            if (!historyStateFilter.value) {
                return historyJobs.value;
            }
            return historyJobs.value.filter(job => job.state === historyStateFilter.value);
        });
        
        const jobDetailVisible = ref(false);
        const currentJob = ref(null);
        const currentJobDetail = ref(null);
        
        const stopJobVisible = ref(false);
        const stoppingJob = ref(false);
        const stopJobForm = reactive({
            jobId: '',
            withSavepoint: true,
            targetDirectory: ''
        });
        
        const restartJobVisible = ref(false);
        const restartingJob = ref(false);
        const restartJobForm = reactive({
            jobId: '',
            savepointPath: ''
        });
        const savepoints = ref([]);
        const restartJobSource = ref('');

        let refreshInterval = null;

        async function fetchApi(url, options = {}) {
            const resp = await fetch(url, {
                headers: { 'Content-Type': 'application/json' },
                ...options
            });
            if (!resp.ok) {
                const text = await resp.text();
                throw new Error(text || resp.statusText);
            }
            return resp.json();
        }

        async function refreshJobs() {
            loadingJobs.value = true;
            try {
                jobs.value = await fetchApi('/api/jobs');
            } catch (e) {
                ElMessage.error('获取作业列表失败: ' + e.message);
            } finally {
                loadingJobs.value = false;
            }
        }

        async function refreshHistoryJobs() {
            loadingHistoryJobs.value = true;
            try {
                const historyData = await fetchApi('/api/jobs/history');
                historyJobs.value = historyData || [];
            } catch (e) {
                ElMessage.error('获取历史作业失败: ' + e.message);
            } finally {
                loadingHistoryJobs.value = false;
            }
        }

        function onJobTabChange(tabName) {
            jobTab.value = tabName;
            if (tabName === 'history') {
                refreshHistoryJobs();
            }
        }

        async function showJobDetail(job) {
            currentJob.value = job;
            jobDetailVisible.value = true;
            try {
                currentJobDetail.value = await fetchApi('/api/jobs/' + job.jid);
            } catch (e) {
                console.error(e);
            }
        }

        async function showHistoryJobDetail(job) {
            currentJob.value = { jid: job.job_id, name: job.job_name || job.flink_job_name };
            jobDetailVisible.value = true;
            try {
                currentJobDetail.value = await fetchApi('/api/jobs/' + job.job_id);
            } catch (e) {
                console.error(e);
            }
        }

        function openStopJobDialog(job) {
            stopJobForm.jobId = job.jid;
            stopJobForm.withSavepoint = true;
            stopJobForm.targetDirectory = '';
            stopJobVisible.value = true;
        }

        async function confirmStopJob() {
            stoppingJob.value = true;
            try {
                const body = stopJobForm.withSavepoint ? {
                    target_directory: stopJobForm.targetDirectory
                } : {};
                await fetchApi('/api/jobs/' + stopJobForm.jobId + '/stop', {
                    method: 'POST',
                    body: JSON.stringify(body)
                });
                ElMessage.success('作业暂停请求已发送');
                stopJobVisible.value = false;
                setTimeout(refreshJobs, 1000);
            } catch (e) {
                ElMessage.error('暂停失败: ' + e.message);
            } finally {
                stoppingJob.value = false;
            }
        }

        async function cancelJob(job) {
            try {
                await ElMessageBox.confirm(
                    '确定要取消作业 "' + (job.name || job.jid) + '" 吗？',
                    '确认取消',
                    {
                        confirmButtonText: '确定',
                        cancelButtonText: '取消',
                        type: 'warning'
                    }
                );
                
                await fetchApi('/api/jobs/' + job.jid + '/cancel', { 
                    method: 'POST'
                });
                
                ElMessage.success('作业取消请求已发送');
                setTimeout(refreshJobs, 1000);
            } catch (e) {
                if (e !== 'cancel') {
                    ElMessage.error('取消失败: ' + e.message);
                }
            }
        }

        async function openRestartJobDialog(job) {
            restartJobSource.value = 'runtime';
            restartJobForm.jobId = job.jid;
            restartJobForm.savepointPath = '';
            savepoints.value = [];
            
            try {
                const result = await fetchApi('/api/jobs/' + job.jid + '/savepoints');
                savepoints.value = result.savepoints || [];
            } catch (e) {
                console.error('获取savepoint失败:', e);
            }
            
            restartJobVisible.value = true;
        }

        async function openHistoryRestartDialog(job) {
            restartJobSource.value = 'history';
            restartJobForm.jobId = job.job_id;
            restartJobForm.savepointPath = job.savepoint_path || '';
            savepoints.value = [];
            
            if (job.job_id) {
                try {
                    const result = await fetchApi('/api/jobs/' + job.job_id + '/savepoints');
                    savepoints.value = result.savepoints || [];
                } catch (e) {
                    console.error('获取savepoint失败:', e);
                }
            }
            
            restartJobVisible.value = true;
        }

        async function confirmRestartJob() {
            if (restartingJob.value) {
                ElMessage.warning('作业正在恢复中，请勿重复点击');
                return;
            }
            
            restartingJob.value = true;
            restartJobVisible.value = false;  // 立即关闭对话框
            
            try {
                const body = {
                    job_id: restartJobForm.jobId,
                    savepoint_path: restartJobForm.savepointPath || null
                };
                
                // 根据来源调用不同的API
                const apiUrl = restartJobSource.value === 'history' 
                    ? '/api/jobs/history/' + restartJobForm.jobId + '/restart'
                    : '/api/jobs/' + restartJobForm.jobId + '/restart';
                
                const response = await fetchApi(apiUrl, {
                    method: 'POST',
                    body: JSON.stringify(body)
                });
                
                // 检查响应状态
                if (response.status === 'running') {
                    // 作业正在启动中
                    ElMessage.success('作业恢复请求已发送，请稍后刷新作业列表');
                } else if (response.job_id || response.jobid) {
                    const newJobId = response.job_id || response.jobid;
                    ElMessage.success('作业已恢复，新作业ID: ' + newJobId);
                } else {
                    ElMessage.success('作业重启请求已发送');
                }
                
                // 延迟刷新列表
                setTimeout(refreshJobs, 2000);
                if (restartJobSource.value === 'history') {
                    setTimeout(refreshHistoryJobs, 2000);
                }
            } catch (e) {
                ElMessage.error('重启失败: ' + e.message);
            } finally {
                restartingJob.value = false;
            }
        }

        async function deleteHistoryJob(job) {
            try {
                await fetchApi('/api/jobs/history/' + job.id, {
                    method: 'DELETE'
                });
                ElMessage.success('删除成功');
                refreshHistoryJobs();
            } catch (e) {
                ElMessage.error('删除失败: ' + e.message);
            }
        }

        onMounted(() => {
            refreshJobs();
            refreshInterval = setInterval(refreshJobs, 10000);
        });

        onUnmounted(() => {
            if (refreshInterval) {
                clearInterval(refreshInterval);
            }
        });

        return {
            jobTab,
            jobs,
            loadingJobs,
            historyJobs,
            loadingHistoryJobs,
            historyStateFilter,
            filteredHistoryJobs,
            jobDetailVisible,
            currentJob,
            currentJobDetail,
            stopJobVisible,
            stoppingJob,
            stopJobForm,
            restartJobVisible,
            restartingJob,
            restartJobForm,
            savepoints,
            refreshJobs,
            refreshHistoryJobs,
            onJobTabChange,
            showJobDetail,
            showHistoryJobDetail,
            openStopJobDialog,
            confirmStopJob,
            cancelJob,
            openRestartJobDialog,
            openHistoryRestartDialog,
            confirmRestartJob,
            deleteHistoryJob,
            formatTime,
            formatDuration,
            formatTimestamp,
            formatJobState,
            getJobStateType
        };
    },
    template: `
<div>
    <el-card>
        <el-tabs v-model="jobTab" @tab-change="onJobTabChange">
            <el-tab-pane label="实时作业" name="running">
                <template #label>
                    <span style="font-size: 16px; font-weight: bold;">实时作业</span>
                </template>
                
                <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 16px;">
                    <span style="font-size: 14px; color: #606266;">当前运行的作业</span>
                    <el-button type="primary" @click="refreshJobs" :loading="loadingJobs" size="small">
                        <el-icon><Refresh /></el-icon> 刷新
                    </el-button>
                </div>
                
                <el-table :data="jobs" style="width: 100%" v-loading="loadingJobs">
                    <el-table-column label="作业名称" min-width="180">
                        <template #default="{ row }">
                            <el-tooltip :content="row.user_job_name || row.name || '-'" placement="top">
                                <span>{{ row.user_job_name || row.name || '-' }}</span>
                            </el-tooltip>
                        </template>
                    </el-table-column>
                    <el-table-column label="Job Name" min-width="300">
                        <template #default="{ row }">
                            <el-tooltip :content="row.name || row.jid" placement="top">
                                <span style="color: #606266;">{{ row.name || row.jid }}</span>
                            </el-tooltip>
                        </template>
                    </el-table-column>
                    <el-table-column prop="jid" label="Job ID" width="300">
                        <template #default="{ row }">
                            <el-link type="primary" @click="showJobDetail(row)">{{ row.jid }}</el-link>
                        </template>
                    </el-table-column>
                    <el-table-column prop="state" label="状态" width="100">
                        <template #default="{ row }">
                            <span class="job-status" :class="row.state">{{ row.state }}</span>
                        </template>
                    </el-table-column>
                    <el-table-column label="开始时间" width="170">
                        <template #default="{ row }">
                            {{ formatTime(row['start-time']) }}
                        </template>
                    </el-table-column>
                    <el-table-column label="运行时长" width="110">
                        <template #default="{ row }">
                            {{ formatDuration(row.duration) }}
                        </template>
                    </el-table-column>
                    <el-table-column label="操作" width="260" fixed="right">
                        <template #default="{ row }">
                            <el-button size="small" @click="showJobDetail(row)">详情</el-button>
                            <template v-if="row.state === 'RUNNING'">
                                <el-button size="small" type="warning" @click="openStopJobDialog(row)">暂停</el-button>
                                <el-button size="small" type="danger" @click="cancelJob(row)">取消</el-button>
                            </template>
                            <el-button
                                v-if="['STOPPED', 'FINISHED', 'FAILED', 'CANCELED'].includes(row.state)"
                                size="small"
                                type="success"
                                @click="openRestartJobDialog(row)"
                            >开启</el-button>
                        </template>
                    </el-table-column>
                </el-table>
            </el-tab-pane>
            
            <el-tab-pane label="历史作业" name="history">
                <template #label>
                    <span style="font-size: 16px; font-weight: bold;">历史作业</span>
                </template>
                
                <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 16px;">
                    <div style="display: flex; align-items: center; gap: 16px;">
                        <span style="font-size: 14px; color: #606266;">历史运行的作业记录</span>
                        <el-select v-model="historyStateFilter" placeholder="按状态筛选" size="small" style="width: 140px;" clearable>
                            <el-option key="all" label="全部" value=""></el-option>
                            <el-option key="FINISHED" label="完成" value="FINISHED"></el-option>
                            <el-option key="FAILED" label="失败" value="FAILED"></el-option>
                            <el-option key="CANCELED" label="取消" value="CANCELED"></el-option>
                            <el-option key="STOPPED" label="暂停" value="STOPPED"></el-option>
                        </el-select>
                    </div>
                    <el-button type="primary" @click="refreshHistoryJobs" :loading="loadingHistoryJobs" size="small">
                        <el-icon><Refresh /></el-icon> 刷新
                    </el-button>
                </div>
                
                <el-table :data="filteredHistoryJobs" style="width: 100%" v-loading="loadingHistoryJobs">
                    <el-table-column label="作业名称" min-width="200">
                        <template #default="{ row }">
                            <el-tooltip :content="row.job_name || row.flink_job_name || row.job_id" placement="top">
                                <span>{{ row.job_name || row.flink_job_name || row.job_id }}</span>
                            </el-tooltip>
                        </template>
                    </el-table-column>
                    <el-table-column prop="job_id" label="Job ID" min-width="250">
                        <template #default="{ row }">
                            <el-link type="primary" @click="showHistoryJobDetail(row)">{{ row.job_id }}</el-link>
                        </template>
                    </el-table-column>
                    <el-table-column prop="state" label="状态" width="100">
                        <template #default="{ row }">
                            <el-tag :type="getJobStateType(row.state)" size="small">
                                {{ formatJobState(row.state) }}
                            </el-tag>
                        </template>
                    </el-table-column>
                    <el-table-column label="开始时间" width="170">
                        <template #default="{ row }">
                            {{ row.start_time ? formatTimestamp(row.start_time) : formatTime(row.created_at) }}
                        </template>
                    </el-table-column>
                    <el-table-column label="结束时间" width="170">
                        <template #default="{ row }">
                            {{ formatTimestamp(row.end_time) }}
                        </template>
                    </el-table-column>
                    <el-table-column label="保存的SQL" width="100">
                        <template #default="{ row }">
                            <el-tag type="success" size="small" v-if="row.sql_text">有</el-tag>
                            <el-tag type="info" size="small" v-else>无</el-tag>
                        </template>
                    </el-table-column>
                    <el-table-column label="Savepoint" width="100">
                        <template #default="{ row }">
                            <el-tag type="warning" size="small" v-if="row.savepoint_path && row.savepoint_timestamp">有</el-tag>
                            <el-tag type="info" size="small" v-else>无</el-tag>
                        </template>
                    </el-table-column>
                    <el-table-column label="操作" width="200">
                        <template #default="{ row }">
                            <el-button size="small" type="primary" @click="openHistoryRestartDialog(row)">
                                重新启动
                            </el-button>
                            <el-popconfirm title="确定删除这条作业记录吗？" @confirm="deleteHistoryJob(row)">
                                <template #reference>
                                    <el-button size="small" type="danger">删除</el-button>
                                </template>
                            </el-popconfirm>
                        </template>
                    </el-table-column>
                </el-table>
            </el-tab-pane>
        </el-tabs>
    </el-card>

    <!-- 作业详情对话框 -->
    <el-dialog v-model="jobDetailVisible" title="作业详情" width="800px">
        <el-descriptions :column="2" border v-if="currentJob">
            <el-descriptions-item label="作业名称">{{ currentJob.name }}</el-descriptions-item>
            <el-descriptions-item label="Job ID">{{ currentJob.jid }}</el-descriptions-item>
            <el-descriptions-item label="状态">
                <span class="job-status" :class="currentJob.state">{{ currentJob.state }}</span>
            </el-descriptions-item>
            <el-descriptions-item label="开始时间">{{ formatTime(currentJob['start-time']) }}</el-descriptions-item>
            <el-descriptions-item label="结束时间">{{ formatTime(currentJob['end-time']) }}</el-descriptions-item>
            <el-descriptions-item label="运行时长">{{ formatDuration(currentJob.duration) }}</el-descriptions-item>
        </el-descriptions>

        <div v-if="currentJob && currentJob.sql_text" style="margin-top: 20px;">
            <h4>SQL 文本</h4>
            <pre style="white-space: pre-wrap; word-break: break-all; max-height: 400px; overflow-y: auto; background-color: #f5f7fa; padding: 10px; border-radius: 4px; border: 1px solid #dcdfe6; font-family: Consolas, Monaco, monospace; font-size: 14px;">{{ currentJob.sql_text }}</pre>
        </div>
        <div v-else-if="currentJobDetail" style="margin-top: 20px;">
            <h4>算子列表</h4>
            <el-table :data="currentJobDetail.vertices || []" style="width: 100%; margin-top: 10px;">
                <el-table-column prop="name" label="算子名称" min-width="200" />
                <el-table-column prop="parallelism" label="并行度" width="80" />
                <el-table-column prop="status" label="状态" width="100" />
            </el-table>
        </div>
    </el-dialog>

    <!-- 暂停作业对话框 -->
    <el-dialog v-model="stopJobVisible" title="暂停作业" width="500px">
        <el-form :model="stopJobForm" label-width="120px">
            <el-form-item label="创建 Savepoint">
                <el-switch v-model="stopJobForm.withSavepoint" />
                <span style="margin-left: 10px; font-size: 12px; color: #666;">
                    保存当前作业状态，可以从该状态恢复
                </span>
            </el-form-item>
            <el-form-item v-if="stopJobForm.withSavepoint" label="Savepoint 路径">
                <el-input v-model="stopJobForm.targetDirectory" placeholder="留空使用默认路径" />
            </el-form-item>
        </el-form>
        <template #footer>
            <el-button @click="stopJobVisible = false">取消</el-button>
            <el-button type="primary" @click="confirmStopJob" :loading="stoppingJob">确认暂停</el-button>
        </template>
    </el-dialog>

    <!-- 恢复作业对话框 -->
    <el-dialog v-model="restartJobVisible" :title="restartJobSource === 'history' ? '从历史作业重新启动' : '从 Savepoint 恢复作业'" width="700px">
        <el-alert v-if="savepoints.length === 0" type="warning" :closable="false" style="margin-bottom: 20px;">
            暂无可用的 Savepoint，将正常启动作业
        </el-alert>
        <el-alert v-else type="info" :closable="false" style="margin-bottom: 20px;">
            检测到 {{ savepoints.length }} 个 Savepoint，可选择从某个 Savepoint 恢复
        </el-alert>
        <el-form :model="restartJobForm" label-width="120px">
            <el-form-item label="Savepoint 路径">
                <el-select v-model="restartJobForm.savepointPath" placeholder="选择 Savepoint 或留空" style="width: 100%;" clearable filterable allow-create>
                    <el-option
                        v-for="sp in savepoints"
                        :key="sp.path"
                        :label="sp.path"
                        :value="sp.path"
                    />
                </el-select>
            </el-form-item>
        </el-form>
        <template #footer>
            <el-button @click="restartJobVisible = false">取消</el-button>
            <el-button type="primary" @click="confirmRestartJob" :loading="restartingJob">确认启动</el-button>
        </template>
    </el-dialog>
</div>
`
};
