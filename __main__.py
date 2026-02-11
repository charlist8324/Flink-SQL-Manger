"""
Flink 管理系统 - 应用入口
"""
import uvicorn
from .backend.main import app

if __name__ == "__main__":
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8088,
        reload=True
    )
