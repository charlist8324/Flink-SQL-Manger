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

function formatSize(bytes) {
    if (!bytes) return 'N/A';
    const units = ['B', 'KB', 'MB', 'GB'];
    let size = bytes;
    let unitIndex = 0;
    while (size >= 1024 && unitIndex < units.length - 1) {
        size /= 1024;
        unitIndex++;
    }
    return size.toFixed(2) + ' ' + units[unitIndex];
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
        const runningJobsNameFilter = ref('');
        
        // 历史作业（按名称去重）
        const historyJobs = ref([]);
        const loadingHistoryJobs = ref(false);
        const historyStateFilter = ref('');
        const historyNameFilter = ref('');
        const historyStartTimeFilter = ref('');
        const historyEndTimeFilter = ref('');
        
        // 运行记录（所有记录）
        const jobRuns = ref([]);
        const loadingJobRuns = ref(false);
        const jobRunsStateFilter = ref('');
        const jobRunsNameFilter = ref('');
        const jobRunsStartTimeFilter = ref('');
        const jobRunsEndTimeFilter = ref('');
        const jobRunsTotal = ref(0);
        const jobRunsPage = ref(1);
        const jobRunsPageSize = ref(20);
        
        // 实时作业过滤
        const filteredJobs = computed(() => {
            if (!runningJobsNameFilter.value) {
                return jobs.value;
            }
            const searchName = runningJobsNameFilter.value.toLowerCase();
            return jobs.value.filter(job => {
                const jobName = (job.user_job_name || job.name || job.jid || '').toLowerCase();
                return jobName.includes(searchName);
            });
        });
        
        const filteredHistoryJobs = computed(() => {
            let filtered = historyJobs.value;
            
            if (historyStateFilter.value) {
                filtered = filtered.filter(job => job.state === historyStateFilter.value);
            }
            
            if (historyNameFilter.value) {
                const searchName = historyNameFilter.value.toLowerCase();
                filtered = filtered.filter(job => {
                    const jobName = (job.job_name || job.flink_job_name || job.job_id || '').toLowerCase();
                    return jobName.includes(searchName);
                });
            }
            
            if (historyStartTimeFilter.value) {
                const startTime = new Date(historyStartTimeFilter.value).getTime();
                filtered = filtered.filter(job => {
                    const jobStart = job.start_time || job.created_at;
                    return jobStart && jobStart >= startTime;
                });
            }
            
            if (historyEndTimeFilter.value) {
                const endTime = new Date(historyEndTimeFilter.value).getTime() + 86400000;
                filtered = filtered.filter(job => {
                    const jobStart = job.start_time || job.created_at;
                    return jobStart && jobStart <= endTime;
                });
            }
            
            return filtered;
        });
        
        const filteredJobRuns = computed(() => {
            let filtered = jobRuns.value;
            
            if (jobRunsStateFilter.value) {
                filtered = filtered.filter(job => job.state === jobRunsStateFilter.value);
            }
            
            if (jobRunsNameFilter.value) {
                const searchName = jobRunsNameFilter.value.toLowerCase();
                filtered = filtered.filter(job => {
                    const jobName = (job.job_name || job.flink_job_name || job.job_id || '').toLowerCase();
                    return jobName.includes(searchName);
                });
            }
            
            if (jobRunsStartTimeFilter.value) {
                const startTime = new Date(jobRunsStartTimeFilter.value).getTime();
                filtered = filtered.filter(job => {
                    const jobStart = job.start_time || job.created_at;
                    return jobStart && jobStart >= startTime;
                });
            }
            
            if (jobRunsEndTimeFilter.value) {
                const endTime = new Date(jobRunsEndTimeFilter.value).getTime() + 86400000;
                filtered = filtered.filter(job => {
                    const jobStart = job.start_time || job.created_at;
                    return jobStart && jobStart <= endTime;
                });
            }
            
            return filtered;
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
        
        const checkpointRestoreVisible = ref(false);
        const restoringJob = ref(false);
        const checkpointRestoreForm = reactive({
            jobId: '',
            checkpointPath: ''
        });
        const checkpoints = ref([]);
        
        const runRecordDetailVisible = ref(false);
        const loadingRunRecordDetail = ref(false);
        const currentRunRecord = ref(null);
        const runRecordCheckpointInfo = ref(null);
        const runRecordSavepointInfo = ref(null);

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
        
        async function refreshJobRuns() {
            loadingJobRuns.value = true;
            try {
                const offset = (jobRunsPage.value - 1) * jobRunsPageSize.value;
                const result = await fetchApi(`/api/jobs/runs?limit=${jobRunsPageSize.value}&offset=${offset}`);
                jobRuns.value = result.jobs || [];
                jobRunsTotal.value = result.total || 0;
            } catch (e) {
                ElMessage.error('获取运行记录失败: ' + e.message);
            } finally {
                loadingJobRuns.value = false;
            }
        }

        function onJobTabChange(tabName) {
            jobTab.value = tabName;
            if (tabName === 'history') {
                refreshHistoryJobs();
            } else if (tabName === 'runs') {
                refreshJobRuns();
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

        async function showRunRecordDetail(job) {
            currentRunRecord.value = job;
            loadingRunRecordDetail.value = true;
            runRecordDetailVisible.value = true;
            
            // 获取checkpoint和savepoint信息
            runRecordCheckpointInfo.value = null;
            runRecordSavepointInfo.value = null;
            
            try {
                // 获取checkpoint信息
                try {
                    const checkpointResult = await fetchApi('/api/jobs/history/' + job.job_id + '/checkpoints');
                    if (checkpointResult.checkpoints && checkpointResult.checkpoints.length > 0) {
                        runRecordCheckpointInfo.value = checkpointResult.checkpoints[0];
                    }
                } catch (e) {
                    console.error('获取checkpoint信息失败:', e);
                }
                
                // 获取savepoint信息
                if (job.savepoint_path) {
                    runRecordSavepointInfo.value = {
                        path: job.savepoint_path,
                        timestamp: job.savepoint_timestamp,
                        format_time: job.savepoint_timestamp ? formatTimestamp(job.savepoint_timestamp) : '-'
                    };
                }
                
            } catch (e) {
                console.error('获取运行记录详情失败:', e);
                ElMessage.error('获取详情失败: ' + e.message);
            } finally {
                loadingRunRecordDetail.value = false;
            }
        }

        async function openCheckpointRestoreDialog(job) {
            checkpointRestoreForm.jobId = job.job_id;
            checkpointRestoreForm.checkpointPath = '';
            checkpoints.value = [];
            
            // 从数据库获取checkpoint列表
            try {
                const result = await fetchApi('/api/jobs/history/' + job.job_id + '/checkpoints');
                if (result.checkpoints && result.checkpoints.length > 0) {
                    checkpoints.value = result.checkpoints.map(cp => ({
                        id: cp.checkpoint_id,
                        path: cp.checkpoint_path,
                        trigger_time: cp.trigger_time,
                        finish_time: cp.finish_time,
                        status: cp.status,
                        size: cp.checkpoint_size,
                        duration: cp.duration
                    }));
                    ElMessage.success(`找到 ${checkpoints.value.length} 个Checkpoint记录`);
                } else {
                    ElMessage.info('该作业没有Checkpoint记录，请手动输入checkpoint路径');
                }
            } catch (e) {
                console.error('获取checkpoint失败:', e);
                ElMessage.warning('获取Checkpoint记录失败，请手动输入checkpoint路径');
            }
            
            checkpointRestoreVisible.value = true;
        }

        async function confirmCheckpointRestore() {
            if (restoringJob.value) {
                ElMessage.warning('作业正在恢复中，请勿重复点击');
                return;
            }
            
            if (!checkpointRestoreForm.checkpointPath) {
                ElMessage.warning('请选择要恢复的checkpoint');
                return;
            }
            
            restoringJob.value = true;
            checkpointRestoreVisible.value = false;
            
            try {
                const body = {
                    job_id: checkpointRestoreForm.jobId,
                    checkpoint_path: checkpointRestoreForm.checkpointPath  // 使用checkpoint_path而不是savepoint_path
                };
                
                const response = await fetchApi('/api/jobs/history/' + checkpointRestoreForm.jobId + '/restart', {
                    method: 'POST',
                    body: JSON.stringify(body)
                });
                
                if (response.status === 'running') {
                    ElMessage.success('作业恢复请求已发送，请稍后刷新作业列表');
                } else if (response.job_id || response.jobid) {
                    const newJobId = response.job_id || response.jobid;
                    ElMessage.success('作业已从checkpoint恢复，新作业ID: ' + newJobId);
                } else {
                    ElMessage.success('作业恢复请求已发送');
                }
                
                setTimeout(refreshHistoryJobs, 2000);
            } catch (e) {
                ElMessage.error('恢复失败: ' + e.message);
            } finally {
                restoringJob.value = false;
            }
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
                await fetchApi('/api/jobs/history/' + job.job_id, {
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
            runningJobsNameFilter,
            filteredJobs,
            historyJobs,
            loadingHistoryJobs,
            historyStateFilter,
            historyNameFilter,
            historyStartTimeFilter,
            historyEndTimeFilter,
            filteredHistoryJobs,
            jobRuns,
            loadingJobRuns,
            jobRunsStateFilter,
            jobRunsNameFilter,
            jobRunsStartTimeFilter,
            jobRunsEndTimeFilter,
            jobRunsTotal,
            jobRunsPage,
            jobRunsPageSize,
            filteredJobRuns,
            jobDetailVisible,
            currentJob,
            currentJobDetail,
            stopJobVisible,
            stoppingJob,
            stopJobForm,
            confirmStopJob,
            cancelJob,
            restartJobVisible,
            restartingJob,
            restartJobForm,
            confirmRestartJob,
            savepoints,
            restartJobSource,
            checkpointRestoreVisible,
            restoringJob,
            checkpointRestoreForm,
            confirmCheckpointRestore,
            checkpoints,
            runRecordDetailVisible,
            loadingRunRecordDetail,
            currentRunRecord,
            runRecordCheckpointInfo,
            runRecordSavepointInfo,
            showRunRecordDetail,
            formatTime,
            formatDuration,
            formatTimestamp,
            formatJobState,
            getJobStateType,
            showJobDetail,
            showHistoryJobDetail,
            openStopJobDialog,
            openRestartJobDialog,
            openCheckpointRestoreDialog,
            openHistoryRestartDialog,
            deleteHistoryJob,
            refreshJobs,
            refreshHistoryJobs,
            refreshJobRuns,
            onJobTabChange,
            formatSize
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
                    <el-input 
                        v-model="runningJobsNameFilter" 
                        placeholder="作业名称" 
                        size="small" 
                        style="width: 200px;" 
                        clearable
                        prefix-icon="Search"
                    />
                    <el-button type="primary" @click="refreshJobs" :loading="loadingJobs" size="small">
                        <el-icon><Refresh /></el-icon> 刷新
                    </el-button>
                </div>
                
                <el-table :data="filteredJobs" style="width: 100%" v-loading="loadingJobs">
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
                    <div style="display: flex; align-items: center; gap: 12px; flex-wrap: wrap;">
                        <el-input 
                            v-model="historyNameFilter" 
                            placeholder="作业名称" 
                            size="small" 
                            style="width: 180px;" 
                            clearable
                            prefix-icon="Search"
                        />
                        
                        <el-select v-model="historyStateFilter" placeholder="状态" size="small" style="width: 120px;" clearable>
                            <el-option key="all" label="全部" value=""></el-option>
                            <el-option key="FINISHED" label="完成" value="FINISHED"></el-option>
                            <el-option key="FAILED" label="失败" value="FAILED"></el-option>
                            <el-option key="CANCELED" label="取消" value="CANCELED"></el-option>
                            <el-option key="STOPPED" label="暂停" value="STOPPED"></el-option>
                        </el-select>
                        
                        <el-date-picker
                            v-model="historyStartTimeFilter"
                            type="date"
                            placeholder="开始时间"
                            size="small"
                            style="width: 150px;"
                            clearable
                            format="YYYY-MM-DD"
                            value-format="YYYY-MM-DD"
                        />
                        
                        <el-date-picker
                            v-model="historyEndTimeFilter"
                            type="date"
                            placeholder="结束时间"
                            size="small"
                            style="width: 150px;"
                            clearable
                            format="YYYY-MM-DD"
                            value-format="YYYY-MM-DD"
                        />
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
                    <el-table-column label="操作" width="300" fixed="right">
                        <template #default="{ row }">
                            <el-button size="small" type="success" @click="openCheckpointRestoreDialog(row)">
                                恢复作业
                            </el-button>
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
            
            <el-tab-pane label="运行记录" name="runs">
                <template #label>
                    <span style="font-size: 16px; font-weight: bold;">运行记录</span>
                </template>
                
                <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 16px;">
                    <div style="display: flex; align-items: center; gap: 12px; flex-wrap: wrap;">
                        <el-input 
                            v-model="jobRunsNameFilter" 
                            placeholder="作业名称" 
                            size="small" 
                            style="width: 180px;" 
                            clearable
                            prefix-icon="Search"
                        />
                        
                        <el-select v-model="jobRunsStateFilter" placeholder="状态" size="small" style="width: 120px;" clearable>
                            <el-option key="all" label="全部" value=""></el-option>
                            <el-option key="FINISHED" label="完成" value="FINISHED"></el-option>
                            <el-option key="FAILED" label="失败" value="FAILED"></el-option>
                            <el-option key="CANCELED" label="取消" value="CANCELED"></el-option>
                            <el-option key="STOPPED" label="暂停" value="STOPPED"></el-option>
                            <el-option key="RUNNING" label="运行中" value="RUNNING"></el-option>
                        </el-select>
                        
                        <el-date-picker
                            v-model="jobRunsStartTimeFilter"
                            type="date"
                            placeholder="开始时间"
                            size="small"
                            style="width: 150px;"
                            clearable
                            format="YYYY-MM-DD"
                            value-format="YYYY-MM-DD"
                        />
                        
                        <el-date-picker
                            v-model="jobRunsEndTimeFilter"
                            type="date"
                            placeholder="结束时间"
                            size="small"
                            style="width: 150px;"
                            clearable
                            format="YYYY-MM-DD"
                            value-format="YYYY-MM-DD"
                        />
                    </div>
                    <el-button type="primary" @click="refreshJobRuns" :loading="loadingJobRuns" size="small">
                        <el-icon><Refresh /></el-icon> 刷新
                    </el-button>
                </div>
                
                <el-table :data="filteredJobRuns" style="width: 100%" v-loading="loadingJobRuns">
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
                    <el-table-column label="操作" width="200" fixed="right">
                        <template #default="{ row }">
                            <el-button size="small" type="primary" @click="showRunRecordDetail(row)">
                                详情
                            </el-button>
                            <el-popconfirm title="确定删除这条作业记录吗？" @confirm="deleteHistoryJob(row)">
                                <template #reference>
                                    <el-button size="small" type="danger">删除</el-button>
                                </template>
                            </el-popconfirm>
                        </template>
                    </el-table-column>
                </el-table>
                
                <div style="margin-top: 16px; display: flex; justify-content: flex-end;">
                    <el-pagination
                        v-model:current-page="jobRunsPage"
                        :page-size="jobRunsPageSize"
                        :total="jobRunsTotal"
                        layout="total, prev, pager, next"
                        @current-change="refreshJobRuns"
                    />
                </div>
            </el-tab-pane>
        </el-tabs>
    </el-card>

    <!-- 作业详情对话框 -->
    <el-dialog v-model="jobDetailVisible" title="作业详情" width="800px">
        <el-descriptions :column="2" border v-if="currentJob" class="job-detail-descriptions">
            <el-descriptions-item label="作业名称">
                <span style="word-break: break-all; white-space: pre-wrap;">{{ currentJob.name }}</span>
            </el-descriptions-item>
            <el-descriptions-item label="Job ID">
                <span style="word-break: break-all;">{{ currentJob.jid }}</span>
            </el-descriptions-item>
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

    <!-- 从Checkpoint恢复作业对话框 -->
    <el-dialog v-model="checkpointRestoreVisible" title="从Checkpoint恢复作业" width="900px">
        <el-alert v-if="checkpoints.length === 0" type="info" :closable="false" style="margin-bottom: 20px;">
            该作业没有可用的Checkpoint列表，请手动输入Checkpoint路径
        </el-alert>
        <el-alert v-else type="success" :closable="false" style="margin-bottom: 20px;">
            检测到 {{ checkpoints.length }} 个Checkpoint记录，可选择从某个Checkpoint恢复
        </el-alert>
        <el-form :model="checkpointRestoreForm" label-width="140px">
            <el-form-item label="Checkpoint路径" v-if="checkpoints.length === 0">
                <el-input v-model="checkpointRestoreForm.checkpointPath" placeholder="请输入Checkpoint完整路径" clearable />
            </el-form-item>
            <div v-else>
                <el-form-item label="选择Checkpoint">
                    <el-select v-model="checkpointRestoreForm.checkpointPath" placeholder="选择要恢复的Checkpoint" style="width: 100%;" clearable filterable allow-create>
                        <el-option
                            v-for="cp in checkpoints"
                            :key="cp.id"
                            :label="'Checkpoint #' + cp.id"
                            :value="cp.path"
                        >
                            <span>Checkpoint #{{ cp.id }}</span>
                            <el-tag :type="cp.status === 'COMPLETED' ? 'success' : 'warning'" size="small" style="margin-left: 8px;">{{ cp.status }}</el-tag>
                        </el-option>
                    </el-select>
                </el-form-item>
                <div v-if="checkpointRestoreForm.checkpointPath" style="margin-top: 8px; padding: 8px; background: #f5f7fa; border-radius: 4px;">
                    <span style="color: #606266; font-size: 12px; word-break: break-all;">{{ checkpointRestoreForm.checkpointPath }}</span>
                </div>
                <el-alert type="info" :closable="false" style="margin-top: 8px;">
                    <template #title>
                        <span style="font-size: 12px;">
                            💡 提示：可以从下拉列表选择Checkpoint，或直接输入/粘贴Checkpoint路径
                        </span>
                    </template>
                </el-alert>
            </div>
        </el-form>
        <template #footer>
            <el-button @click="checkpointRestoreVisible = false">取消</el-button>
            <el-button type="primary" @click="confirmCheckpointRestore" :loading="restoringJob" :disabled="!checkpointRestoreForm.checkpointPath">确认恢复</el-button>
        </template>
    </el-dialog>
    <!-- 运行记录详情对话框 -->
    <el-dialog v-model="runRecordDetailVisible" title="运行记录详情" width="900px" v-loading="loadingRunRecordDetail">
        <div v-if="currentRunRecord">
            <el-descriptions :column="2" border>
                <el-descriptions-item label="作业名称">
                    <span style="word-break: break-all;">{{ currentRunRecord.job_name || currentRunRecord.flink_job_name || '-' }}</span>
                </el-descriptions-item>
                <el-descriptions-item label="Job ID">
                    <span style="word-break: break-all;">{{ currentRunRecord.job_id }}</span>
                </el-descriptions-item>
                <el-descriptions-item label="状态">
                    <el-tag :type="getJobStateType(currentRunRecord.state)" size="small">
                        {{ formatJobState(currentRunRecord.state) }}
                    </el-tag>
                </el-descriptions-item>
                <el-descriptions-item label="开始时间">
                    {{ currentRunRecord.start_time ? formatTimestamp(currentRunRecord.start_time) : formatTime(currentRunRecord.created_at) }}
                </el-descriptions-item>
                <el-descriptions-item label="结束时间">
                    {{ formatTimestamp(currentRunRecord.end_time) }}
                </el-descriptions-item>
                <el-descriptions-item label="保存的SQL">
                    <el-tag type="success" size="small" v-if="currentRunRecord.sql_text">有</el-tag>
                    <el-tag type="info" size="small" v-else>无</el-tag>
                </el-descriptions-item>
            </el-descriptions>
            
            <div style="margin-top: 20px;" v-if="currentRunRecord.sql_text">
                <h4 style="margin-bottom: 10px; color: #303133;">SQL 文本</h4>
                <pre style="white-space: pre-wrap; word-break: break-all; max-height: 200px; overflow-y: auto; background-color: #f5f7fa; padding: 12px; border-radius: 4px; border: 1px solid #dcdfe6; font-family: Consolas, Monaco, monospace; font-size: 13px; color: #606266;">{{ currentRunRecord.sql_text }}</pre>
            </div>
            
            <div style="margin-top: 20px;">
                <h4 style="margin-bottom: 10px; color: #303133;">Checkpoint 状态</h4>
                <div v-if="runRecordCheckpointInfo" style="background-color: #f0f9ff; padding: 15px; border-radius: 4px; border: 1px solid #b3d8ff;">
                    <el-descriptions :column="2" size="small">
                        <el-descriptions-item label="Checkpoint ID">
                            <span style="font-weight: bold; color: #409eff;">{{ runRecordCheckpointInfo.checkpoint_id }}</span>
                        </el-descriptions-item>
                        <el-descriptions-item label="状态">
                            <el-tag :type="runRecordCheckpointInfo.status === 'COMPLETED' ? 'success' : 'warning'" size="small">
                                {{ runRecordCheckpointInfo.status }}
                            </el-tag>
                        </el-descriptions-item>
                        <el-descriptions-item label="Checkpoint 路径" :span="2">
                            <span style="word-break: break-all; color: #409eff; font-family: Consolas, Monaco, monospace; font-size: 13px;">{{ runRecordCheckpointInfo.checkpoint_path || '-' }}</span>
                        </el-descriptions-item>
                        <el-descriptions-item label="触发时间">
                            {{ formatTimestamp(runRecordCheckpointInfo.trigger_time) }}
                        </el-descriptions-item>
                        <el-descriptions-item label="完成时间">
                            {{ formatTimestamp(runRecordCheckpointInfo.finish_time) }}
                        </el-descriptions-item>
                        <el-descriptions-item label="Checkpoint 大小" v-if="runRecordCheckpointInfo.checkpoint_size">
                            {{ formatSize(runRecordCheckpointInfo.checkpoint_size) }}
                        </el-descriptions-item>
                        <el-descriptions-item label="耗时" v-if="runRecordCheckpointInfo.duration">
                            {{ runRecordCheckpointInfo.duration }} ms
                        </el-descriptions-item>
                    </el-descriptions>
                </div>
                <div v-else style="background-color: #f4f4f5; padding: 15px; border-radius: 4px; text-align: center; color: #909399;">
                    <el-icon><InfoFilled /></el-icon>
                    <span style="margin-left: 8px;">该作业没有Checkpoint记录</span>
                </div>
            </div>
            
            <div style="margin-top: 20px;">
                <h4 style="margin-bottom: 10px; color: #303133;">Savepoint 状态</h4>
                <div v-if="runRecordSavepointInfo" style="background-color: #f0f9ff; padding: 15px; border-radius: 4px; border: 1px solid #b3d8ff;">
                    <el-descriptions :column="2" size="small">
                        <el-descriptions-item label="Savepoint 路径" :span="2">
                            <span style="word-break: break-all; color: #67c23a; font-family: Consolas, Monaco, monospace; font-size: 13px;">{{ runRecordSavepointInfo.path }}</span>
                        </el-descriptions-item>
                        <el-descriptions-item label="保存时间">
                            {{ runRecordSavepointInfo.format_time }}
                        </el-descriptions-item>
                        <el-descriptions-item label="时间戳">
                            {{ runRecordSavepointInfo.timestamp }}
                        </el-descriptions-item>
                    </el-descriptions>
                </div>
                <div v-else style="background-color: #f4f4f5; padding: 15px; border-radius: 4px; text-align: center; color: #909399;">
                    <el-icon><InfoFilled /></el-icon>
                    <span style="margin-left: 8px;">该作业没有Savepoint记录</span>
                </div>
            </div>
        </div>
    </el-dialog>
</div>
`
};
