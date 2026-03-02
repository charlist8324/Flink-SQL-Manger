const { ref, reactive, onMounted } = Vue;
const { ElMessage, ElMessageBox } = ElementPlus;

export default {
    setup() {
        const configs = ref([]);
        const loadingConfigs = ref(false);
        const configDialogVisible = ref(false);
        const savingConfig = ref(false);
        
        const configForm = reactive({
            config_key: '',
            config_value: '',
            description: ''
        });

        async function fetchConfigs() {
            loadingConfigs.value = true;
            try {
                const response = await fetch('/api/flink-config');
                if (response.ok) {
                    const data = await response.json();
                    configs.value = data || [];
                }
            } catch (error) {
                ElMessage.error('获取配置列表失败: ' + error.message);
            } finally {
                loadingConfigs.value = false;
            }
        }

        function openConfigDialog(config = null) {
            if (config) {
                configForm.config_key = config.config_key;
                configForm.config_value = config.config_value;
                configForm.description = config.description || '';
            } else {
                configForm.config_key = '';
                configForm.config_value = '';
                configForm.description = '';
            }
            configDialogVisible.value = true;
        }

        async function saveConfig() {
            if (!configForm.config_key || !configForm.config_value) {
                ElMessage.warning('请填写配置键和配置值');
                return;
            }
            
            savingConfig.value = true;
            try {
                const response = await fetch('/api/flink-config', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(configForm)
                });
                
                if (!response.ok) {
                    const text = await response.text();
                    throw new Error(text || response.statusText);
                }
                
                ElMessage.success('配置保存成功');
                configDialogVisible.value = false;
                fetchConfigs();
            } catch (error) {
                ElMessage.error('保存配置失败: ' + error.message);
            } finally {
                savingConfig.value = false;
            }
        }

        async function deleteConfig(config) {
            try {
                await ElMessageBox.confirm(
                    '确定要删除配置 "' + config.config_key + '" 吗？',
                    '确认删除',
                    {
                        confirmButtonText: '确定',
                        cancelButtonText: '取消',
                        type: 'warning'
                    }
                );
                
                const response = await fetch('/api/flink-config/' + config.config_key, {
                    method: 'DELETE'
                });
                
                if (!response.ok) {
                    const text = await response.text();
                    throw new Error(text || response.statusText);
                }
                
                ElMessage.success('配置删除成功');
                fetchConfigs();
            } catch (error) {
                if (error !== 'cancel') {
                    ElMessage.error('删除配置失败: ' + error.message);
                }
            }
        }

        function getConfigDisplayName(configKey) {
            const displayNames = {
                'checkpoint_path': 'Checkpoint路径',
                'savepoint_path': 'Savepoint路径'
            };
            return displayNames[configKey] || configKey;
        }

        function getConfigPlaceholder(configKey) {
            const placeholders = {
                'checkpoint_path': '例如: hdfs:///flink/checkpoints',
                'savepoint_path': '例如: hdfs:///flink/savepoints'
            };
            return placeholders[configKey] || '请输入配置值';
        }

        onMounted(() => {
            fetchConfigs();
        });

        return {
            configs,
            loadingConfigs,
            configDialogVisible,
            savingConfig,
            configForm,
            fetchConfigs,
            openConfigDialog,
            saveConfig,
            deleteConfig,
            getConfigDisplayName,
            getConfigPlaceholder
        };
    },
    template: `
    <div class="flink-config-container">
        <el-card>
            <template #header>
                <div style="display: flex; justify-content: space-between; align-items: center;">
                    <span>Flink配置</span>
                    <el-button type="primary" size="small" @click="openConfigDialog()">
                        <el-icon><Plus /></el-icon>
                        添加配置
                    </el-button>
                </div>
            </template>
            
            <el-table :data="configs" v-loading="loadingConfigs" style="width: 100%;">
                <el-table-column prop="config_key" label="配置键" width="200">
                    <template #default="{ row }">
                        {{ getConfigDisplayName(row.config_key) }}
                    </template>
                </el-table-column>
                <el-table-column prop="config_value" label="配置值" min-width="300">
                    <template #default="{ row }">
                        <span style="font-family: monospace; font-size: 13px;">{{ row.config_value }}</span>
                    </template>
                </el-table-column>
                <el-table-column prop="description" label="描述" width="200" />
                <el-table-column prop="updated_at" label="更新时间" width="170">
                    <template #default="{ row }">
                        {{ new Date(row.updated_at).toLocaleString('zh-CN') }}
                    </template>
                </el-table-column>
                <el-table-column label="操作" width="150" fixed="right">
                    <template #default="{ row }">
                        <el-button size="small" type="primary" link @click="openConfigDialog(row)">
                            编辑
                        </el-button>
                        <el-button size="small" type="danger" link @click="deleteConfig(row)">
                            删除
                        </el-button>
                    </template>
                </el-table-column>
            </el-table>
            
            <el-empty v-if="!loadingConfigs && configs.length === 0" description="暂无配置" />
        </el-card>
        
        <el-dialog v-model="configDialogVisible" :title="configForm.config_key ? '编辑配置' : '添加配置'" width="600px">
            <el-form :model="configForm" label-width="120px">
                <el-form-item label="配置键">
                    <el-select v-model="configForm.config_key" placeholder="选择配置键" style="width: 100%;" filterable allow-create>
                        <el-option label="Checkpoint路径" value="checkpoint_path" />
                        <el-option label="Savepoint路径" value="savepoint_path" />
                        <el-option label="自定义配置" value="" disabled />
                    </el-select>
                    <div style="margin-top: 8px; color: #909399; font-size: 12px;">
                        提示：可以从下拉列表选择，或直接输入自定义配置键
                    </div>
                </el-form-item>
                <el-form-item label="配置值">
                    <el-input 
                        v-model="configForm.config_value" 
                        :placeholder="getConfigPlaceholder(configForm.config_key)"
                        type="textarea"
                        :rows="3"
                    />
                </el-form-item>
                <el-form-item label="描述">
                    <el-input v-model="configForm.description" placeholder="请输入配置描述（可选）" />
                </el-form-item>
            </el-form>
            <template #footer>
                <el-button @click="configDialogVisible = false">取消</el-button>
                <el-button type="primary" @click="saveConfig" :loading="savingConfig">
                    保存
                </el-button>
            </template>
        </el-dialog>
    </div>
    `
};
