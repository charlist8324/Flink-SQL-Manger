const { ref, reactive, onMounted } = Vue;
const { ElMessage, ElMessageBox } = ElementPlus;

export default {
    setup() {
        const datasources = ref([]);
        const loadingDatasources = ref(false);
        const datasourceDialogVisible = ref(false);
        const savingDatasource = ref(false);
        const testingConnection = ref(false);
        
        const datasourceForm = reactive({
            id: null,
            name: '',
            type: '',
            host: '',
            port: 3306,
            database: '',
            username: '',
            password: '',
            properties: {}
        });

        // 获取数据源列表
        async function fetchDatasources() {
            loadingDatasources.value = true;
            try {
                const response = await fetch('/api/datasources');
                if (response.ok) {
                    const data = await response.json();
                    datasources.value = data || [];
                }
            } catch (error) {
                ElMessage.error('获取数据源列表失败: ' + error.message);
            } finally {
                loadingDatasources.value = false;
            }
        }

        function openDatasourceDialog(datasource = null) {
            if (datasource) {
                Object.assign(datasourceForm, datasource);
                datasourceForm.properties = datasource.properties || {};
            } else {
                datasourceForm.id = null;
                datasourceForm.name = '';
                datasourceForm.type = '';
                datasourceForm.host = '';
                datasourceForm.port = 3306;
                datasourceForm.database = '';
                datasourceForm.username = '';
                datasourceForm.password = '';
                datasourceForm.properties = {};
            }
            datasourceDialogVisible.value = true;
        }

        // 测试连接 - 按照旧版实现
        async function testConnection() {
            if (!datasourceForm.type || !datasourceForm.host || !datasourceForm.port) {
                ElMessage.warning('请至少填写类型、主机和端口');
                return;
            }
            
            testingConnection.value = true;
            try {
                // 只发送需要的字段
                const testData = {
                    name: datasourceForm.name || 'test',
                    type: datasourceForm.type,
                    host: datasourceForm.host,
                    port: datasourceForm.port,
                    database: datasourceForm.database,
                    username: datasourceForm.username,
                    password: datasourceForm.password,
                    properties: {}
                };
                
                const response = await fetch('/api/datasources/test', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify(testData)
                });
                
                if (!response.ok) {
                    const text = await response.text();
                    throw new Error(text || response.statusText);
                }
                
                const result = await response.json();
                
                if (result.status === 'success') {
                    ElMessage.success(result.message || '连接成功');
                } else {
                    ElMessage.error(result.message || '连接失败');
                }
            } catch (error) {
                ElMessage.error('测试连接失败: ' + error.message);
            } finally {
                testingConnection.value = false;
            }
        }

        // 测试已有数据源连接
        async function testExistingDatasource(row) {
            try {
                // 只发送需要的字段
                const testData = {
                    name: row.name || 'test',
                    type: row.type,
                    host: row.host,
                    port: row.port,
                    database: row.database,
                    username: row.username,
                    password: row.password,
                    properties: row.properties || {}
                };
                
                const response = await fetch('/api/datasources/test', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify(testData)
                });
                
                if (!response.ok) {
                    const text = await response.text();
                    throw new Error(text || response.statusText);
                }
                
                const result = await response.json();
                
                if (result.status === 'success') {
                    ElMessage.success(result.message || '连接成功');
                } else {
                    ElMessage.error(result.message || '连接失败');
                }
            } catch (error) {
                ElMessage.error('测试连接失败: ' + error.message);
            }
        }

        async function saveDatasource() {
            savingDatasource.value = true;
            try {
                const url = datasourceForm.id ? `/api/datasources/${datasourceForm.id}` : '/api/datasources';
                const method = datasourceForm.id ? 'PUT' : 'POST';
                
                const response = await fetch(url, {
                    method: method,
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify(datasourceForm)
                });
                
                if (!response.ok) {
                    const text = await response.text();
                    throw new Error(text || response.statusText);
                }
                
                const data = await response.json();
                
                if (data.status === 'success') {
                    ElMessage.success('保存成功');
                    datasourceDialogVisible.value = false;
                    fetchDatasources();
                } else {
                    ElMessage.error(data.message || '保存失败');
                }
            } catch (error) {
                ElMessage.error('保存失败: ' + error.message);
            } finally {
                savingDatasource.value = false;
            }
        }

        async function deleteDatasource(row) {
            try {
                await ElMessageBox.confirm('确定要删除该数据源吗？', '提示', {
                    confirmButtonText: '确定',
                    cancelButtonText: '取消',
                    type: 'warning'
                });
                
                const response = await fetch(`/api/datasources/${row.id}`, {
                    method: 'DELETE'
                });
                
                if (!response.ok) {
                    const text = await response.text();
                    throw new Error(text || response.statusText);
                }
                
                const data = await response.json();
                
                if (data.status === 'success') {
                    ElMessage.success('删除成功');
                    fetchDatasources();
                } else {
                    ElMessage.error(data.message || '删除失败');
                }
            } catch (error) {
                if (error !== 'cancel') {
                    ElMessage.error('删除失败: ' + error.message);
                }
            }
        }

        function handleTypeChange() {
            const type = datasourceForm.type;
            if (type === 'mysql') {
                datasourceForm.port = 3306;
            } else if (type === 'oracle') {
                datasourceForm.port = 1521;
            } else if (type === 'postgresql') {
                datasourceForm.port = 5432;
            } else if (type === 'sqlserver') {
                datasourceForm.port = 1433;
            } else if (type === 'doris' || type === 'starrocks') {
                datasourceForm.port = 9030;
            } else if (type === 'clickhouse') {
                datasourceForm.port = 8123;
            }
        }

        onMounted(() => {
            fetchDatasources();
        });

        return {
            datasources,
            loadingDatasources,
            datasourceDialogVisible,
            savingDatasource,
            testingConnection,
            datasourceForm,
            fetchDatasources,
            openDatasourceDialog,
            testConnection,
            testExistingDatasource,
            saveDatasource,
            deleteDatasource,
            handleTypeChange
        };
    },
    template: `
<div>
    <div style="margin-bottom: 20px; display: flex; justify-content: space-between; align-items: center;">
        <span style="font-size: 16px; font-weight: bold;">数据源列表</span>
        <el-button type="primary" @click="openDatasourceDialog()">
            <el-icon><Plus /></el-icon> 添加数据源
        </el-button>
    </div>
    
    <el-table :data="datasources" v-loading="loadingDatasources" style="width: 100%" border stripe>
        <el-table-column prop="name" label="别名" width="180" show-overflow-tooltip />
        <el-table-column prop="type" label="类型" width="100">
            <template #default="{ row }">
                <el-tag>{{ row.type }}</el-tag>
            </template>
        </el-table-column>
        <el-table-column label="连接信息" min-width="250" show-overflow-tooltip>
            <template #default="{ row }">
                <span>{{ row.host }}:{{ row.port }} / {{ row.database }}</span>
            </template>
        </el-table-column>
        <el-table-column prop="username" label="用户名" width="120" show-overflow-tooltip />
        <el-table-column label="操作" width="250" fixed="right">
            <template #default="{ row }">
                <el-button type="success" link @click="testExistingDatasource(row)">测试</el-button>
                <el-button type="primary" link @click="openDatasourceDialog(row)">编辑</el-button>
                <el-button type="danger" link @click="deleteDatasource(row)">删除</el-button>
            </template>
        </el-table-column>
    </el-table>

    <el-dialog v-model="datasourceDialogVisible" :title="datasourceForm.id ? '编辑数据源' : '添加数据源'" width="600px">
        <el-form :model="datasourceForm" label-width="100px">
            <el-form-item label="别名" required>
                <el-input v-model="datasourceForm.name" placeholder="用于识别数据源" />
            </el-form-item>
            <el-form-item label="类型" required>
                <el-select v-model="datasourceForm.type" placeholder="选择类型" @change="handleTypeChange" style="width: 100%;">
                    <el-option label="MySQL" value="mysql" />
                    <el-option label="Oracle" value="oracle" />
                    <el-option label="PostgreSQL" value="postgresql" />
                    <el-option label="SQL Server" value="sqlserver" />
                    <el-option label="Doris" value="doris" />
                    <el-option label="StarRocks" value="starrocks" />
                    <el-option label="ClickHouse" value="clickhouse" />
                </el-select>
            </el-form-item>
            <el-form-item label="主机" required>
                <el-input v-model="datasourceForm.host" placeholder="IP地址或主机名" />
            </el-form-item>
            <el-form-item label="端口" required>
                <el-input-number v-model="datasourceForm.port" :min="1" :max="65535" style="width: 100%;" />
            </el-form-item>
            <el-form-item label="数据库" required>
                <el-input v-model="datasourceForm.database" placeholder="数据库名称" />
            </el-form-item>
            <el-form-item label="用户名" required>
                <el-input v-model="datasourceForm.username" placeholder="数据库用户名" />
            </el-form-item>
            <el-form-item label="密码" required>
                <el-input v-model="datasourceForm.password" type="password" placeholder="数据库密码" show-password />
            </el-form-item>
        </el-form>
        <template #footer>
            <el-button @click="datasourceDialogVisible = false">取消</el-button>
            <el-button type="success" @click="testConnection" :loading="testingConnection">测试连接</el-button>
            <el-button type="primary" @click="saveDatasource" :loading="savingDatasource">保存</el-button>
        </template>
    </el-dialog>
</div>
`
};
