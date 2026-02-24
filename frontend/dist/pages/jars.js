import { fetchApi, formatTime } from '../common.js';

const { ref, onMounted, ElMessage, ElMessageBox } = Vue;

export default {
    setup() {
        const jars = ref([]);
        const loading = ref(false);
        const uploadDialogVisible = ref(false);
        const uploading = ref(false);
        const selectedFile = ref(null);

        // 获取Jar列表
        async function fetchJars() {
            loading.value = true;
            try {
                const data = await fetchApi('/api/jars');
                if (data.status === 'success') {
                    jars.value = data.data || [];
                }
            } catch (error) {
                ElMessage.error('获取Jar列表失败: ' + error.message);
            } finally {
                loading.value = false;
            }
        }

        // 文件选择
        function onFileChange(event) {
            selectedFile.value = event.target.files[0];
        }

        // 上传Jar
        async function uploadJar() {
            if (!selectedFile.value) {
                ElMessage.warning('请选择Jar文件');
                return;
            }

            uploading.value = true;
            const formData = new FormData();
            formData.append('jar', selectedFile.value);

            try {
                const response = await fetch('/api/jars/upload', {
                    method: 'POST',
                    body: formData
                });
                
                const data = await response.json();
                if (data.status === 'success') {
                    ElMessage.success('上传成功');
                    uploadDialogVisible.value = false;
                    fetchJars();
                } else {
                    ElMessage.error(data.message || '上传失败');
                }
            } catch (error) {
                ElMessage.error('上传失败: ' + error.message);
            } finally {
                uploading.value = false;
                selectedFile.value = null;
            }
        }

        // 删除Jar
        async function deleteJar(jarId) {
            try {
                await ElMessageBox.confirm('确定要删除该Jar包吗？', '提示', {
                    confirmButtonText: '确定',
                    cancelButtonText: '取消',
                    type: 'warning'
                });
                
                const data = await fetchApi(`/api/jars/${jarId}`, {
                    method: 'DELETE'
                });
                
                if (data.status === 'success') {
                    ElMessage.success('删除成功');
                    fetchJars();
                } else {
                    ElMessage.error(data.message || '删除失败');
                }
            } catch (error) {
                if (error !== 'cancel') {
                    ElMessage.error('删除失败: ' + error.message);
                }
            }
        }

        onMounted(() => {
            fetchJars();
        });

        return {
            jars,
            loading,
            uploadDialogVisible,
            uploading,
            selectedFile,
            formatTime,
            fetchJars,
            onFileChange,
            uploadJar,
            deleteJar
        };
    },
    template: `
<div>
    <el-card>
        <template #header>
            <div style="display: flex; justify-content: space-between; align-items: center;">
                <span>Jar 管理</span>
                <el-button type="primary" @click="uploadDialogVisible = true">
                    <el-icon><Upload /></el-icon> 上传Jar
                </el-button>
            </div>
        </template>
        
        <el-table :data="jars" style="width: 100%" v-loading="loading">
            <el-table-column prop="name" label="Jar名称" min-width="200" />
            <el-table-column prop="size" label="大小" width="120" />
            <el-table-column label="上传时间" width="180">
                <template #default="{ row }">
                    {{ formatTime(row.upload_time) }}
                </template>
            </el-table-column>
            <el-table-column label="操作" width="120" fixed="right">
                <template #default="{ row }">
                    <el-button size="small" type="danger" @click="deleteJar(row.id)">删除</>
                    </>
                </template>
            </el-table-column>
        </el-table>
    </el-card>

    <!-- 上传对话框 -->
    <el-dialog v-model="uploadDialogVisible" title="上传Jar包" width="500px">
        <el-form label-width="80px">
            <el-form-item label="选择文件">
                <el-input type="file" @change="onFileChange" accept=".jar" />
            </el-form-item>
        </el-form>
        <template #footer>
            <el-button @click="uploadDialogVisible = false">取消</el-button>
            <el-button type="primary" @click="uploadJar" :loading="uploading">上传</el-button>
        </template>
    </el-dialog>
</div>
    `
};