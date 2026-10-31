# AutoResearch

## 产品介绍

AutoResearch 是在本机运行的自动化科研工作台。多个 AI 成员协作完成研究探索、方案审查、代码实现、实验验证与报告生成，可在网页中查看讨论、投票、执行进度和证据。

支持多条研究线并行、暂停与终止、历史回放及故障恢复。研究数据默认保存在本机 `data/` 目录，真实研究使用 DeepSeek API；也可选择演示模式体验流程。

## 部署方法

环境：Windows、Python 3.12+、Node.js 22.12+、[uv](https://docs.astral.sh/uv/)。

在 PowerShell 中运行：

```powershell
git clone --branch release --single-branch https://github.com/lzclzclzclzclzclzc/AutoResearch.git
cd AutoResearch
powershell -ExecutionPolicy Bypass -File scripts/setup.ps1
powershell -ExecutionPolicy Bypass -File start.ps1
```

打开 [本机工作台](http://127.0.0.1:8000)，在“模型与凭据”中保存 DeepSeek API Key，再配置研究团队并创建任务。安装脚本会安装后端依赖与官方 DSH 运行时，并构建前端；日常启动只需运行 `start.ps1`。

默认端口为 `8000`（网页）、`8766`（控制）和 `8767`（实验执行），服务仅监听本机。可通过启动参数修改端口和数据目录：

```powershell
powershell -ExecutionPolicy Bypass -File start.ps1 --port 8080 --control-port 8866 --runner-port 8867 --data-dir D:/ResearchData
```

按 `Ctrl+C` 停止服务与在途研究。备份前先停止服务，再复制整个数据目录；迁移到其他 Windows 用户或机器后需重新配置 API Key。
