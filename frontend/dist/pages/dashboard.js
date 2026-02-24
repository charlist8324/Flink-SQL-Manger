const { ref, onMounted } = Vue;

export default {
    setup() {
        const clusterStatus = ref({
            jobs_running: 0,
            jobs_finished: 0,
            jobs_cancelled: 0,
            jobs_failed: 0,
            taskmanagers: 0,
            slots_total: 0,
            slots_available: 0
        });
        const recentJobs = ref([]);

        // 时间格式化
        function formatTime(timestamp) {
            if (!timestamp) return '-';
            const date = new Date(timestamp);
            return date.toLocaleString('zh-CN');
        }

        // 刷新集群状态
        async function refreshClusterStatus() {
            try {
                const response = await fetch('/api/cluster/status');
                if (!response.ok) throw new Error('请求失败');
                const data = await response.json();
                Object.assign(clusterStatus.value, data);
            } catch (error) {
                console.error('获取集群状态失败:', error);
            }
        }

        // 刷新作业列表
        async function refreshJobs() {
            try {
                const response = await fetch('/api/jobs');
                if (!response.ok) throw new Error('请求失败');
                const data = await response.json();
                recentJobs.value = Array.isArray(data) ? data : [];
            } catch (error) {
                console.error('获取作业列表失败:', error);
            }
        }

        onMounted(() => {
            refreshClusterStatus();
            refreshJobs();
            setInterval(refreshClusterStatus, 10000);
            setInterval(refreshJobs, 10000);
        });

        return {
            clusterStatus,
            recentJobs,
            formatTime
        };
    },
    template: `
<div>
    <el-row :gutter="20">
        <el-col :span="6">
            <el-card class="dashboard-card">
                <div class="stat-card">
                    <div class="number">{{ clusterStatus.jobs_running }}</div>
                    <div class="label">运行中作业</div>
                </div>
            </el-card>
        </el-col>
        <el-col :span="6">
            <el-card class="dashboard-card">
                <div class="stat-card success">
                    <div class="number">{{ clusterStatus.jobs_finished }}</div>
                    <div class="label">已完成作业</div>
                </div>
            </el-card>
        </el-col>
        <el-col :span="6">
            <el-card class="dashboard-card">
                <div class="stat-card warning">
                    <div class="number">{{ clusterStatus.jobs_cancelled }}</div>
                    <div class="label">已取消作业</div>
                </div>
            </el-card>
        </el-col>
        <el-col :span="6">
            <el-card class="dashboard-card">
                <div class="stat-card danger">
                    <div class="number">{{ clusterStatus.jobs_failed }}</div>
                    <div class="label">失败作业</div>
                </div>
            </el-card>
        </el-col>
    </el-row>

    <el-row :gutter="20">
        <el-col :span="8">
            <el-card class="dashboard-card">
                <div class="stat-card">
                    <div class="number">{{ clusterStatus.taskmanagers }}</div>
                    <div class="label">TaskManager 数量</div>
                </div>
            </el-card>
        </el-col>
        <el-col :span="8">
            <el-card class="dashboard-card">
                <div class="stat-card">
                    <div class="number">{{ clusterStatus.slots_total }}</div>
                    <div class="label">总 Slots</div>
                </div>
            </el-card>
        </el-col>
        <el-col :span="8">
            <el-card class="dashboard-card">
                <div class="stat-card success">
                    <div class="number">{{ clusterStatus.slots_available }}</div>
                    <div class="label">可用 Slots</div>
                </div>
            </el-card>
        </el-col>
    </el-row>

    <el-card style="margin-top: 20px;">
        <template #header>
            <span>最近作业</span>
        </template>
        <el-table :data="recentJobs.slice(0, 5)" style="width: 100%">
            <el-table-column label="作业名称" min-width="200">
                <template #default="{ row }">
                    <span :title="row.name">{{ row.name || row.jid }}</span>
                </template>
            </el-table-column>
            <el-table-column prop="jid" label="Job ID" width="320" />
            <el-table-column prop="state" label="状态" width="120">
                <template #default="{ row }">
                    <span class="job-status" :class="row.state">{{ row.state }}</span>
                </template>
            </el-table-column>
            <el-table-column label="开始时间" width="180">
                <template #default="{ row }">
                    {{ formatTime(row['start-time']) }}
                </template>
            </el-table-column>
        </el-table>
    </el-card>
</div>
    `
};