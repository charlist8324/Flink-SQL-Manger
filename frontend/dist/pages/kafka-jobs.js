const { ref, reactive, computed, onMounted } = Vue;
const { ElMessage, ElMessageBox } = ElementPlus;

function formatTime(timestamp) {
    if (!timestamp) return '-';
    return new Date(timestamp).toLocaleString('zh-CN');
}

function formatJobState(state) {
    const stateMap = {
        'CREATED': '已创建',
        'RUNNING': '运行中',
        'STOPPED': '已停止',
        'FAILED': '失败'
    };
    return stateMap[state] || state;
}

function getJobStateType(state) {
    const typeMap = {
        'CREATED': '',
        'RUNNING': 'success',
        'STOPPED': 'warning',
        'FAILED': 'danger'
    };
    return typeMap[state] || 'info';
}

export default {
    setup() {
        const jobs = ref([]);
        const loading = ref(false);
        const datasources = ref([]);
        const kafkaDatasources = ref([]);
        const dbDatasources = ref([]);
        
        const dialogVisible = ref(false);
        const dialogTitle = ref('新建Kafka同步作业');
        const saving = ref(false);
        
        const form = reactive({
            id: null,
            job_name: '',
            source_datasource_id: null,
            source_topics: '',
            source_group_id: '',
            source_start_mode: 'latest',
            source_format: 'json',
            source_schema: [],
            target_datasource_id: null,
            target_table: '',
            target_database: '',
            auto_create_table: true,
            table_primary_keys: '',
            field_mappings: [],
            parallelism: 1,
            checkpoint_interval: 60000
        });
        
        const schemaDialogVisible = ref(false);
        const newColumn = reactive({
            name: '',
            type: 'STRING'
        });
        
        const columnTypes = [
            'STRING', 'VARCHAR(255)', 'INT', 'BIGINT', 'FLOAT', 'DOUBLE', 
            'BOOLEAN', 'DATE', 'TIMESTAMP', 'DECIMAL(10,2)', 'BYTES'
        ];
        
        async function fetchJobs() {
            loading.value = true;
            try {
                const response = await fetch('/api/kafka-jobs');
                jobs.value = await response.json();
            } catch (error) {
                ElMessage.error('获取作业列表失败: ' + error.message);
            } finally {
                loading.value = false;
            }
        }
        
        async function fetchDatasources() {
            try {
                const response = await fetch('/api/datasources');
                const data = await response.json();
                datasources.value = data;
                kafkaDatasources.value = data.filter(ds => ds.type === 'kafka');
                dbDatasources.value = data.filter(ds => ['mysql', 'postgresql', 'doris', 'starrocks', 'oracle', 'sqlserver', 'clickhouse'].includes(ds.type));
            } catch (error) {
                ElMessage.error('获取数据源列表失败: ' + error.message);
            }
        }
        
        function openDialog(job = null) {
            if (job) {
                dialogTitle.value = '编辑Kafka同步作业';
                Object.assign(form, {
                    id: job.id,
                    job_name: job.job_name,
                    source_datasource_id: job.source_datasource_id,
                    source_topics: job.source_topics,
                    source_group_id: job.source_group_id || '',
                    source_start_mode: job.source_start_mode || 'latest',
                    source_format: job.source_format || 'json',
                    source_schema: job.source_schema || [],
                    target_datasource_id: job.target_datasource_id,
                    target_table: job.target_table,
                    target_database: job.target_database || '',
                    auto_create_table: job.auto_create_table !== false,
                    table_primary_keys: job.table_primary_keys || '',
                    field_mappings: job.field_mappings || [],
                    parallelism: job.parallelism || 1,
                    checkpoint_interval: job.checkpoint_interval || 60000
                });
            } else {
                dialogTitle.value = '新建Kafka同步作业';
                Object.assign(form, {
                    id: null,
                    job_name: '',
                    source_datasource_id: null,
                    source_topics: '',
                    source_group_id: '',
                    source_start_mode: 'latest',
                    source_format: 'json',
                    source_schema: [],
                    target_datasource_id: null,
                    target_table: '',
                    target_database: '',
                    auto_create_table: true,
                    table_primary_keys: '',
                    field_mappings: [],
                    parallelism: 1,
                    checkpoint_interval: 60000
                });
            }
            dialogVisible.value = true;
        }
        
        async function saveJob() {
            if (!form.job_name || !form.source_datasource_id || !form.source_topics || 
                !form.target_datasource_id || !form.target_table) {
                ElMessage.warning('请填写必填项');
                return;
            }
            
            saving.value = true;
            try {
                const url = form.id ? `/api/kafka-jobs/${form.id}` : '/api/kafka-jobs';
                const method = form.id ? 'PUT' : 'POST';
                
                const body = { ...form };
                delete body.id;
                
                const response = await fetch(url, {
                    method,
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(body)
                });
                
                const result = await response.json();
                if (result.status === 'success') {
                    ElMessage.success('保存成功');
                    dialogVisible.value = false;
                    fetchJobs();
                } else {
                    ElMessage.error(result.message || '保存失败');
                }
            } catch (error) {
                ElMessage.error('保存失败: ' + error.message);
            } finally {
                saving.value = false;
            }
        }
        
        async function deleteJob(job) {
            try {
                await ElMessageBox.confirm(`确定删除作业 "${job.job_name}" 吗？`, '确认删除', {
                    type: 'warning'
                });
                
                const response = await fetch(`/api/kafka-jobs/${job.id}`, { method: 'DELETE' });
                const result = await response.json();
                if (result.status === 'success') {
                    ElMessage.success('删除成功');
                    fetchJobs();
                } else {
                    ElMessage.error(result.message || '删除失败');
                }
            } catch (error) {
                if (error !== 'cancel') {
                    ElMessage.error('删除失败: ' + error.message);
                }
            }
        }
        
        async function startJob(job) {
            try {
                const response = await fetch(`/api/kafka-jobs/${job.id}/start`, { method: 'POST' });
                const result = await response.json();
                if (result.status === 'success') {
                    ElMessage.success('作业已启动');
                    fetchJobs();
                } else {
                    ElMessage.error(result.detail || '启动失败');
                }
            } catch (error) {
                ElMessage.error('启动失败: ' + error.message);
            }
        }
        
        async function stopJob(job) {
            try {
                await ElMessageBox.confirm(`确定停止作业 "${job.job_name}" 吗？`, '确认停止', {
                    type: 'warning'
                });
                
                const response = await fetch(`/api/kafka-jobs/${job.id}/stop`, { method: 'POST' });
                const result = await response.json();
                if (result.status === 'success') {
                    ElMessage.success('作业已停止');
                    fetchJobs();
                } else {
                    ElMessage.error(result.detail || '停止失败');
                }
            } catch (error) {
                if (error !== 'cancel') {
                    ElMessage.error('停止失败: ' + error.message);
                }
            }
        }
        
        function addColumn() {
            if (!newColumn.name || !newColumn.type) {
                ElMessage.warning('请填写字段名和类型');
                return;
            }
            form.source_schema.push({ ...newColumn });
            newColumn.name = '';
            newColumn.type = 'STRING';
        }
        
        function removeColumn(index) {
            form.source_schema.splice(index, 1);
        }
        
        function generateMappings() {
            form.field_mappings = form.source_schema.map(col => ({
                source: col.name,
                target: col.name,
                type: col.type
            }));
        }
        
        onMounted(() => {
            fetchJobs();
            fetchDatasources();
        });
        
        return {
            jobs,
            loading,
            datasources,
            kafkaDatasources,
            dbDatasources,
            dialogVisible,
            dialogTitle,
            saving,
            form,
            schemaDialogVisible,
            newColumn,
            columnTypes,
            fetchJobs,
            openDialog,
            saveJob,
            deleteJob,
            startJob,
            stopJob,
            addColumn,
            removeColumn,
            generateMappings,
            formatTime,
            formatJobState,
            getJobStateType
        };
    },
    template: `
<div>
    <div style="margin-bottom: 20px; display: flex; justify-content: space-between; align-items: center;">
        <span style="font-size: 16px; font-weight: bold;">Kafka同步作业</span>
        <el-button type="primary" @click="openDialog()">
            <el-icon><Plus /></el-icon> 新建作业
        </el-button>
    </div>
    
    <el-table :data="jobs" v-loading="loading" stripe>
        <el-table-column prop="job_name" label="作业名称" min-width="150" />
        <el-table-column label="源(Kafka)" min-width="180">
            <template #default="{ row }">
                <div>{{ row.source_datasource_name }}</div>
                <div style="color: #909399; font-size: 12px;">{{ row.source_topics }}</div>
            </template>
        </el-table-column>
        <el-table-column label="目标(数据库)" min-width="180">
            <template #default="{ row }">
                <div>{{ row.target_datasource_name }}</div>
                <div style="color: #909399; font-size: 12px;">{{ row.target_table }}</div>
            </template>
        </el-table-column>
        <el-table-column prop="parallelism" label="并行度" width="80" />
        <el-table-column label="状态" width="100">
            <template #default="{ row }">
                <el-tag :type="getJobStateType(row.state)" size="small">{{ formatJobState(row.state) }}</el-tag>
            </template>
        </el-table-column>
        <el-table-column label="创建时间" width="160">
            <template #default="{ row }">{{ formatTime(row.created_at) }}</template>
        </el-table-column>
        <el-table-column label="操作" width="200" fixed="right">
            <template #default="{ row }">
                <el-button type="success" link @click="startJob(row)" v-if="row.state !== 'RUNNING'">启动</el-button>
                <el-button type="warning" link @click="stopJob(row)" v-if="row.state === 'RUNNING'">停止</el-button>
                <el-button type="primary" link @click="openDialog(row)">编辑</el-button>
                <el-button type="danger" link @click="deleteJob(row)">删除</el-button>
            </template>
        </el-table-column>
    </el-table>
    
    <el-dialog v-model="dialogVisible" :title="dialogTitle" width="800px">
        <el-form :model="form" label-width="120px">
            <el-divider content-position="left">基本信息</el-divider>
            <el-row :gutter="20">
                <el-col :span="12">
                    <el-form-item label="作业名称" required>
                        <el-input v-model="form.job_name" placeholder="输入作业名称" />
                    </el-form-item>
                </el-col>
                <el-col :span="6">
                    <el-form-item label="并行度">
                        <el-input-number v-model="form.parallelism" :min="1" :max="100" style="width: 100%;" />
                    </el-form-item>
                </el-col>
                <el-col :span="6">
                    <el-form-item label="Checkpoint">
                        <el-input-number v-model="form.checkpoint_interval" :min="1000" :step="1000" style="width: 100%;" />
                        <div style="color: #909399; font-size: 12px;">毫秒</div>
                    </el-form-item>
                </el-col>
            </el-row>
            
            <el-divider content-position="left">源端配置 (Kafka)</el-divider>
            <el-row :gutter="20">
                <el-col :span="12">
                    <el-form-item label="Kafka数据源" required>
                        <el-select v-model="form.source_datasource_id" placeholder="选择Kafka数据源" style="width: 100%;">
                            <el-option v-for="ds in kafkaDatasources" :key="ds.id" :label="ds.name" :value="ds.id" />
                        </el-select>
                    </el-form-item>
                </el-col>
                <el-col :span="12">
                    <el-form-item label="Topics" required>
                        <el-input v-model="form.source_topics" placeholder="topic1,topic2 多个用逗号分隔" />
                    </el-form-item>
                </el-col>
            </el-row>
            <el-row :gutter="20">
                <el-col :span="8">
                    <el-form-item label="消费者组">
                        <el-input v-model="form.source_group_id" placeholder="可选" />
                    </el-form-item>
                </el-col>
                <el-col :span="8">
                    <el-form-item label="启动模式">
                        <el-select v-model="form.source_start_mode" style="width: 100%;">
                            <el-option label="最新" value="latest" />
                            <el-option label="最早" value="earliest" />
                            <el-option label="时间戳" value="timestamp" />
                        </el-select>
                    </el-form-item>
                </el-col>
                <el-col :span="8">
                    <el-form-item label="数据格式">
                        <el-select v-model="form.source_format" style="width: 100%;">
                            <el-option label="JSON" value="json" />
                            <el-option label="CSV" value="csv" />
                            <el-option label="Avro" value="avro" />
                        </el-select>
                    </el-form-item>
                </el-col>
            </el-row>
            
            <el-form-item label="Schema定义">
                <div style="width: 100%;">
                    <div v-for="(col, index) in form.source_schema" :key="index" style="display: flex; gap: 10px; margin-bottom: 8px; align-items: center;">
                        <el-input v-model="col.name" placeholder="字段名" style="width: 200px;" />
                        <el-select v-model="col.type" style="width: 150px;">
                            <el-option v-for="t in columnTypes" :key="t" :label="t" :value="t" />
                        </el-select>
                        <el-button type="danger" link @click="removeColumn(index)">删除</el-button>
                    </div>
                    <div style="display: flex; gap: 10px; margin-top: 10px;">
                        <el-input v-model="newColumn.name" placeholder="字段名" style="width: 200px;" />
                        <el-select v-model="newColumn.type" style="width: 150px;">
                            <el-option v-for="t in columnTypes" :key="t" :label="t" :value="t" />
                        </el-select>
                        <el-button type="primary" @click="addColumn">添加字段</el-button>
                        <el-button @click="generateMappings" v-if="form.source_schema.length > 0">生成映射</el-button>
                    </div>
                </div>
            </el-form-item>
            
            <el-divider content-position="left">目标端配置 (数据库)</el-divider>
            <el-row :gutter="20">
                <el-col :span="12">
                    <el-form-item label="目标数据源" required>
                        <el-select v-model="form.target_datasource_id" placeholder="选择目标数据库" style="width: 100%;">
                            <el-option v-for="ds in dbDatasources" :key="ds.id" :label="ds.name + ' (' + ds.type + ')'" :value="ds.id" />
                        </el-select>
                    </el-form-item>
                </el-col>
                <el-col :span="12">
                    <el-form-item label="目标表名" required>
                        <el-input v-model="form.target_table" placeholder="目标表名" />
                    </el-form-item>
                </el-col>
            </el-row>
            <el-row :gutter="20">
                <el-col :span="12">
                    <el-form-item label="目标数据库">
                        <el-input v-model="form.target_database" placeholder="可选，留空使用数据源默认" />
                    </el-form-item>
                </el-col>
                <el-col :span="12">
                    <el-form-item label="主键字段">
                        <el-input v-model="form.table_primary_keys" placeholder="id,name 多个用逗号分隔" />
                    </el-form-item>
                </el-col>
            </el-row>
            <el-form-item label="自动建表">
                <el-switch v-model="form.auto_create_table" />
                <span style="color: #909399; font-size: 12px; margin-left: 10px;">如果目标表不存在，自动创建</span>
            </el-form-item>
            
            <el-divider content-position="left" v-if="form.field_mappings.length > 0">字段映射</el-divider>
            <el-form-item label="字段映射" v-if="form.field_mappings.length > 0">
                <el-table :data="form.field_mappings" size="small" style="width: 100%;">
                    <el-table-column prop="source" label="源字段" />
                    <el-table-column prop="target" label="目标字段">
                        <template #default="{ row }">
                            <el-input v-model="row.target" size="small" />
                        </template>
                    </el-table-column>
                    <el-table-column prop="type" label="类型" width="120" />
                </el-table>
            </el-form-item>
        </el-form>
        <template #footer>
            <el-button @click="dialogVisible = false">取消</el-button>
            <el-button type="primary" @click="saveJob" :loading="saving">保存</el-button>
        </template>
    </el-dialog>
</div>
`
};
