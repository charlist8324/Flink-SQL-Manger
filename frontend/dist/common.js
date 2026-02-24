// 通用API请求函数
async function fetchApi(url, options = {}) {
    const defaultOptions = {
        method: 'GET',
        headers: {
            'Content-Type': 'application/json'
        }
    };
    
    const mergedOptions = { ...defaultOptions, ...options };
    
    try {
        const response = await fetch(url, mergedOptions);
        const data = await response.json();
        return data;
    } catch (error) {
        console.error(`API请求失败 ${url}:`, error);
        throw error;
    }
}

// 时间格式化
function formatTime(timestamp) {
    if (!timestamp) return '-';
    const date = new Date(timestamp);
    return date.toLocaleString('zh-CN');
}

// 状态样式映射
const jobStatusClass = {
    'RUNNING': 'running',
    'FINISHED': 'finished',
    'FAILED': 'failed',
    'CANCELED': 'canceled',
    'RESTARTING': 'restarting',
    'INITIALIZING': 'initializing'
};

export {
    fetchApi,
    formatTime,
    jobStatusClass
};