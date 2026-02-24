const { ref, onMounted, ElMessage, ElDialog } = Vue;

export default {
    setup() {
        const clusterStatus = ref({
            status: 'offline',
            flink_version: '',
            jobs_running: 0,
            jobs_finished: 0,
            jobs_cancelled: 0,
            jobs_failed: 0,
            taskmanagers: 0,
            slots_total: 0,
            slots_available: 0
        });
        const taskManagers = ref([]);
        const loading = ref(false);
        const tmMetricsVisible = ref(false);
        const currentTmId = ref(null);
        const currentTmMetrics = ref([]);
        const loadingTmMetrics = ref(false);

        // 时间格式化
        function formatTime(timestamp) {
            if (!timestamp) return '-';
            const date = new Date(timestamp);
            return date.toLocaleString('zh-CN');
        }

        // 刷新集群状态
        async function refreshClusterStatus() {
            loading.value = true;
            try {
                const response = await fetch('/api/cluster/status');
                if (response.ok) {
                    const data = await response.json();
                    Object.assign(clusterStatus.value, data);
                    clusterStatus.value.status = 'online';
                } else {
                    clusterStatus.value.status = 'offline';
                }
            } catch (error) {
                console.error('获取集群状态失败:', error);
                clusterStatus.value.status = 'offline';
            } finally {
                loading.value = false;
            }
        }

        // 刷新TaskManager列表
        async function refreshTaskManagers() {
            loading.value = true;
            try {
                const tmResponse = await fetch('/api/cluster/taskmanagers');
                if (tmResponse.ok) {
                    const tmData = await tmResponse.json();
                    taskManagers.value = Array.isArray(tmData) ? tmData : [];
                }
            } catch (error) {
                console.error('获取TaskManager列表失败:', error);
            } finally {
                loading.value = false;
            }
        }

        // 显示TaskManager监控
        async function showTmMetrics(row) {
            tmMetricsVisible.value = true;
            currentTmId.value = row.id;
            loadingTmMetrics.value = true;
            currentTmMetrics.value = [];
            
            try {
                const metrics = [
                    'Status.JVM.CPU.Load',
                    'Status.JVM.Threads.Count',
                    'Status.JVM.Memory.Heap.Used',
                    'Status.JVM.Memory.Heap.Max',
                    'Status.JVM.Memory.Heap.Committed',
                    'Status.JVM.Memory.NonHeap.Used',
                    'Status.JVM.Memory.NonHeap.Max',
                    'Status.JVM.Memory.NonHeap.Committed',
                    'Status.JVM.Memory.Direct.Used',
                    'Status.JVM.Memory.Direct.Max',
                    'Status.JVM.Memory.Direct.Count',
                    'Status.JVM.Memory.Mapped.Used',
                    'Status.JVM.Memory.Mapped.Max',
                    'Status.JVM.Memory.Mapped.Count',
                    'Status.JVM.GarbageCollector.G1_Young_Generation.Count',
                    'Status.JVM.GarbageCollector.G1_Young_Generation.Time',
                    'Status.JVM.GarbageCollector.G1_Old_Generation.Count',
                    'Status.JVM.GarbageCollector.G1_Old_Generation.Time',
                    'Status.JVM.GarbageCollector.PS_Scavenge.Count',
                    'Status.JVM.GarbageCollector.PS_Scavenge.Time',
                    'Status.JVM.GarbageCollector.PS_MarkSweep.Count',
                    'Status.JVM.GarbageCollector.PS_MarkSweep.Time',
                    'Status.JVM.GarbageCollector.ParNew.Count',
                    'Status.JVM.GarbageCollector.ParNew.Time',
                    'Status.JVM.GarbageCollector.ConcurrentMarkSweep.Count',
                    'Status.JVM.GarbageCollector.ConcurrentMarkSweep.Time'
                ];
                
                const response = await fetch('/api/cluster/taskmanagers/' + row.id + '/metrics?metrics=' + metrics.join(','));
                if (response.ok) {
                    const data = await response.json();
                    currentTmMetrics.value = data || [];
                }
            } catch (error) {
                console.error('获取监控数据失败:', error);
            } finally {
                loadingTmMetrics.value = false;
            }
        }

        // 获取指标值
        function getMetricValue(id) {
            if (!currentTmMetrics.value || currentTmMetrics.value.length === 0) return 0;
            const metric = currentTmMetrics.value.find(m => m.id === id);
            if (!metric || metric.value === null || metric.value === undefined) return 0;
            const value = parseFloat(metric.value);
            return isNaN(value) ? 0 : value;
        }

        // 字节格式化
        function formatBytes(bytes) {
            if (bytes === 0 || !bytes || isNaN(bytes)) return '0 B';
            const k = 1024;
            const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
            const i = Math.floor(Math.log(bytes) / Math.log(k));
            return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
        }

        // 百分比格式化
        function formatPercent(value) {
            if (!value || isNaN(value)) return '0%';
            return (value * 100).toFixed(2) + '%';
        }

        onMounted(() => {
            refreshClusterStatus();
            refreshTaskManagers();
            setInterval(refreshClusterStatus, 10000);
            setInterval(refreshTaskManagers, 10000);
        });

        return {
            clusterStatus,
            taskManagers,
            loading,
            tmMetricsVisible,
            currentTmId,
            currentTmMetrics,
            loadingTmMetrics,
            formatTime,
            refreshClusterStatus,
            refreshTaskManagers,
            showTmMetrics,
            getMetricValue,
            formatBytes,
            formatPercent
        };
    },
    template: `
<div>
    <el-card class="dashboard-card" style="margin-bottom: 20px;">
        <template #header>
            <span>集群配置</span>
        </template>
        <el-descriptions :column="4" border>
            <el-descriptions-item label="Flink 版本" label-align="right" align="center">{{ clusterStatus.flink_version || '-' }}</el-descriptions-item>
            <el-descriptions-item label="集群状态" label-align="right" align="center">
                <span class="cluster-status">
                    <span class="dot" :class="clusterStatus.status"></span>
                    {{ clusterStatus.status === 'online' ? '在线' : '离线' }}
                </span>
            </el-descriptions-item>
            <el-descriptions-item label="TaskManager 数量" label-align="right" align="center">{{ clusterStatus.taskmanagers }}</el-descriptions-item>
            <el-descriptions-item label="Slots (总/可用)" label-align="right" align="center">{{ clusterStatus.slots_total }} / {{ clusterStatus.slots_available }}</el-descriptions-item>
        </el-descriptions>
    </el-card>

    <el-card>
        <template #header>
            <div style="display: flex; justify-content: space-between; align-items: center;">
                <span>TaskManager 列表</span>
                <el-button type="primary" @click="refreshTaskManagers" :loading="loading">
                    <el-icon><Refresh /></el-icon> 刷新
                </el-button>
            </div>
        </template>
        
        <el-table :data="taskManagers" style="width: 100%" v-loading="loading">
            <el-table-column prop="id" label="TaskManager ID" min-width="280" show-overflow-tooltip></el-table-column>
            <el-table-column prop="path" label="地址" min-width="180" show-overflow-tooltip></el-table-column>
            <el-table-column prop="dataPort" label="数据端口" width="100"></el-table-column>
            <el-table-column prop="slotsNumber" label="Slots" width="80"></el-table-column>
            <el-table-column prop="freeSlots" label="空闲 Slots" width="100"></el-table-column>
            <el-table-column label="操作" width="100" fixed="right">
                <template #default="{ row }">
                    <el-button type="primary" link @click="showTmMetrics(row)">
                        监控
                    </el-button>
                </template>
            </el-table-column>
        </el-table>
    </el-card>

    <!-- TaskManager监控对话框 -->
    <el-dialog v-model="tmMetricsVisible" :title="'TaskManager 监控 - ' + currentTmId" width="900px">
        <div v-if="currentTmMetrics.length > 0" style="display: grid; grid-template-columns: 1fr 1fr; gap: 20px;">
            <!-- JVM Heap -->
            <el-card>
                <template #header>
                    <span>JVM 内存 (Heap)</span>
                </template>
                <div style="display: flex; flex-direction: column; gap: 8px;">
                    <div style="display: flex; justify-content: space-between;">
                        <span>已用:</span>
                        <span>{{ formatBytes(getMetricValue('Status.JVM.Memory.Heap.Used')) }}</span>
                    </div>
                    <div style="display: flex; justify-content: space-between;">
                        <span>最大:</span>
                        <span>{{ formatBytes(getMetricValue('Status.JVM.Memory.Heap.Max')) }}</span>
                    </div>
                    <div style="display: flex; justify-content: space-between;">
                        <span>提交:</span>
                        <span>{{ formatBytes(getMetricValue('Status.JVM.Memory.Heap.Committed')) }}</span>
                    </div>
                </div>
            </el-card>

            <!-- JVM Non-Heap -->
            <el-card>
                <template #header>
                    <span>JVM 内存 (Non-Heap)</span>
                </template>
                <div style="display: flex; flex-direction: column; gap: 8px;">
                    <div style="display: flex; justify-content: space-between;">
                        <span>已用:</span>
                        <span>{{ formatBytes(getMetricValue('Status.JVM.Memory.NonHeap.Used')) }}</span>
                    </div>
                    <div style="display: flex; justify-content: space-between;">
                        <span>最大:</span>
                        <span>{{ formatBytes(getMetricValue('Status.JVM.Memory.NonHeap.Max')) }}</span>
                    </div>
                    <div style="display: flex; justify-content: space-between;">
                        <span>提交:</span>
                        <span>{{ formatBytes(getMetricValue('Status.JVM.Memory.NonHeap.Committed')) }}</span>
                    </div>
                </div>
            </el-card>

            <!-- JVM Direct -->
            <el-card>
                <template #header>
                    <span>JVM 内存 (Direct)</span>
                </template>
                <div style="display: flex; flex-direction: column; gap: 8px;">
                    <div style="display: flex; justify-content: space-between;">
                        <span>已用:</span>
                        <span>{{ formatBytes(getMetricValue('Status.JVM.Memory.Direct.Used')) }}</span>
                    </div>
                    <div style="display: flex; justify-content: space-between;">
                        <span>最大:</span>
                        <span>{{ formatBytes(getMetricValue('Status.JVM.Memory.Direct.Max')) }}</span>
                    </div>
                    <div style="display: flex; justify-content: space-between;">
                        <span>Count:</span>
                        <span>{{ getMetricValue('Status.JVM.Memory.Direct.Count') }}</span>
                    </div>
                </div>
            </el-card>

            <!-- JVM Mapped -->
            <el-card>
                <template #header>
                    <span>JVM 内存 (Mapped)</span>
                </template>
                <div style="display: flex; flex-direction: column; gap: 8px;">
                    <div style="display: flex; justify-content: space-between;">
                        <span>已用:</span>
                        <span>{{ formatBytes(getMetricValue('Status.JVM.Memory.Mapped.Used')) }}</span>
                    </div>
                    <div style="display: flex; justify-content: space-between;">
                        <span>最大:</span>
                        <span>{{ formatBytes(getMetricValue('Status.JVM.Memory.Mapped.Max')) }}</span>
                    </div>
                    <div style="display: flex; justify-content: space-between;">
                        <span>Count:</span>
                        <span>{{ getMetricValue('Status.JVM.Memory.Mapped.Count') }}</span>
                    </div>
                </div>
            </el-card>

            <!-- CPU & 线程 -->
            <el-card>
                <template #header>
                    <span>CPU & 线程</span>
                </template>
                <div style="display: flex; flex-direction: column; gap: 8px;">
                    <div style="display: flex; justify-content: space-between;">
                        <span>CPU 负载:</span>
                        <span>{{ formatPercent(getMetricValue('Status.JVM.CPU.Load')) }}</span>
                    </div>
                    <div style="display: flex; justify-content: space-between;">
                        <span>线程数:</span>
                        <span>{{ getMetricValue('Status.JVM.Threads.Count') }}</span>
                    </div>
                </div>
            </el-card>

            <!-- 垃圾回收 -->
            <el-card>
                <template #header>
                    <span>垃圾回收 (GC)</span>
                </template>
                <div style="display: flex; flex-direction: column; gap: 8px;">
                    <div style="display: flex; justify-content: space-between;">
                        <span>Young GC Count:</span>
                        <span>{{ getMetricValue('Status.JVM.GarbageCollector.G1_Young_Generation.Count') || getMetricValue('Status.JVM.GarbageCollector.PS_Scavenge.Count') || getMetricValue('Status.JVM.GarbageCollector.ParNew.Count') }}</span>
                    </div>
                    <div style="display: flex; justify-content: space-between;">
                        <span>Young GC Time:</span>
                        <span>{{ getMetricValue('Status.JVM.GarbageCollector.G1_Young_Generation.Time') || getMetricValue('Status.JVM.GarbageCollector.PS_Scavenge.Time') || getMetricValue('Status.JVM.GarbageCollector.ParNew.Time') }} ms</span>
                    </div>
                    <div style="display: flex; justify-content: space-between;">
                        <span>Old GC Count:</span>
                        <span>{{ getMetricValue('Status.JVM.GarbageCollector.G1_Old_Generation.Count') || getMetricValue('Status.JVM.GarbageCollector.PS_MarkSweep.Count') || getMetricValue('Status.JVM.GarbageCollector.ConcurrentMarkSweep.Count') }}</span>
                    </div>
                    <div style="display: flex; justify-content: space-between;">
                        <span>Old GC Time:</span>
                        <span>{{ getMetricValue('Status.JVM.GarbageCollector.G1_Old_Generation.Time') || getMetricValue('Status.JVM.GarbageCollector.PS_MarkSweep.Time') || getMetricValue('Status.JVM.GarbageCollector.ConcurrentMarkSweep.Time') }} ms</span>
                    </div>
                </div>
            </el-card>
        </div>
        <div v-else style="text-align: center; padding: 40px; color: #999;" v-loading="loadingTmMetrics">
            暂无监控数据
        </div>
        <template #footer>
            <el-button @click="tmMetricsVisible = false">关闭</el-button>
        </template>
    </el-dialog>
</div>
    `
};