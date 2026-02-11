# Flink SQL 管理系统 - 自动安装并启动脚本

Write-Host "======================================" -ForegroundColor Cyan
Write-Host "Flink SQL 管理系统启动脚本" -ForegroundColor Cyan
Write-Host "======================================" -ForegroundColor Cyan
Write-Host ""

$PYTHON_DIR = ".\python_portable"
$PYTHON_EXE = "$PYTHON_DIR\python.exe"

# 检查是否已经有便携版 Python
if (-Not (Test-Path $PYTHON_EXE)) {
    Write-Host "[1/4] 未检测到 Python，正在下载便携版 Python 3.11..." -ForegroundColor Yellow
    
    $PYTHON_ZIP = "python-embed.zip"
    $PYTHON_URL = "https://mirrors.huaweicloud.com/python/3.11.9/python-3.11.9-embed-amd64.zip"
    
    # 下载 Python
    try {
        Invoke-WebRequest -Uri $PYTHON_URL -OutFile $PYTHON_ZIP -UseBasicParsing
        Write-Host "    下载完成！" -ForegroundColor Green
    } catch {
        Write-Host "    下载失败，尝试官方源..." -ForegroundColor Red
        $PYTHON_URL = "https://www.python.org/ftp/python/3.11.9/python-3.11.9-embed-amd64.zip"
        Invoke-WebRequest -Uri $PYTHON_URL -OutFile $PYTHON_ZIP -UseBasicParsing
    }
    
    # 解压
    Write-Host "[2/4] 正在解压 Python..." -ForegroundColor Yellow
    Expand-Archive -Path $PYTHON_ZIP -DestinationPath $PYTHON_DIR -Force
    Remove-Item $PYTHON_ZIP
    
    # 修改 Python 配置以支持 pip
    $PTH_FILE = Get-ChildItem "$PYTHON_DIR\python*._pth" | Select-Object -First 1
    if ($PTH_FILE) {
        $content = Get-Content $PTH_FILE.FullName
        $content = $content -replace '#import site', 'import site'
        Set-Content $PTH_FILE.FullName $content
    }
    
    # 下载并安装 pip
    Write-Host "[3/4] 正在安装 pip..." -ForegroundColor Yellow
    Invoke-WebRequest -Uri "https://bootstrap.pypa.io/get-pip.py" -OutFile "get-pip.py"
    & $PYTHON_EXE get-pip.py
    Remove-Item get-pip.py
    
    Write-Host "    Python 安装完成！" -ForegroundColor Green
} else {
    Write-Host "[1/4] 检测到已有 Python" -ForegroundColor Green
}

# 安装依赖
Write-Host ""
Write-Host "[2/4] 正在安装项目依赖..." -ForegroundColor Yellow
& $PYTHON_EXE -m pip install --upgrade pip -q
& $PYTHON_EXE -m pip install -r requirements.txt -q
Write-Host "    依赖安装完成！" -ForegroundColor Green

# 检查配置
Write-Host ""
Write-Host "[3/4] 检查配置..." -ForegroundColor Yellow
$config_content = Get-Content "backend\config.py" -Raw
if ($config_content -match 'your_sql_runner_jar_id.jar') {
    Write-Host "    警告: 尚未配置 SQL_RUNNER_JAR_ID" -ForegroundColor Red
    Write-Host "    请先编辑 backend\config.py，配置正确的 SQL Runner Jar ID" -ForegroundColor Yellow
} else {
    Write-Host "    配置检查完成！" -ForegroundColor Green
}

# 启动服务
Write-Host ""
Write-Host "[4/4] 正在启动后端服务..." -ForegroundColor Yellow
Write-Host ""
Write-Host "======================================" -ForegroundColor Green
Write-Host "服务将在以下地址运行:" -ForegroundColor Green
Write-Host "  - 本地访问: http://localhost:8000" -ForegroundColor Cyan
Write-Host "  - API 文档: http://localhost:8000/docs" -ForegroundColor Cyan
Write-Host "  - 健康检查: http://localhost:8000/health" -ForegroundColor Cyan
Write-Host "======================================" -ForegroundColor Green
Write-Host ""
Write-Host "按 Ctrl+C 停止服务" -ForegroundColor Yellow
Write-Host ""

& $PYTHON_EXE -m uvicorn backend.main:app --host 0.0.0.0 --port 8000 --reload
