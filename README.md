# AutoResearch

在本机运行的自动化科研工作台。实现依据为 [design.md](docs/design.md) 与 [technical-implementation.md](docs/technical-implementation.md)。

包含 React 工作台、FastAPI API、LangGraph 编排 Worker、独立 Control Supervisor、独立 Runner、SQLite 持久化，以及官方 DeepSeek Harness Python SDK 适配。研究数据默认保存在项目下的 `data/`，服务只监听回环地址。

## 安装与启动

当前主要验证环境为 Windows、Python 3.13、Node.js 24。Python 要求 3.12 及以上；前端构建要求 Node.js 20.19+ 或 22.12+。安装脚本需要 [uv](https://docs.astral.sh/uv/)。

```powershell
cd C:\Users\64379\Desktop\Programs\AutoResearch
powershell -ExecutionPolicy Bypass -File scripts/setup.ps1
.venv/Scripts/python.exe scripts/launch.py
```

打开 [本机工作台](http://127.0.0.1:8000)。也可运行 `start.ps1`。启动器先开放独立停止通道，再启动网页 API、Runner 与 Worker。通过 Ctrl+C 正常退出启动器时，会终止其管理的在途工作。

前端已经构建时，日常运行只需要 Python 与官方 DSH 运行时，无需再启动 Vite。首次安装下载官方 Windows 运行时可能较慢；可设置 `$env:UV_HTTP_TIMEOUT='300'` 后重试安装。

默认端口：网页/API `8000`、Control `8766`、Runner `8767`。可以通过启动器的 `--port`、`--control-port`、`--runner-port` 改变。已有端口被占用时启动器会拒绝覆盖现有进程。

```powershell
# 独立数据目录，不与已有研究混用
.venv/Scripts/python.exe scripts/launch.py --data-dir D:/ResearchData --no-browser

# 前端开发（API 保持运行）
cd apps/web
npm ci
npm run dev
```

## 开始研究

1. 在“模型与凭据”保存 DeepSeek API Key，测试连接。默认显示 DeepSeek V4.1 Flash，请求模型 ID 为 `deepseek-flash`。
2. 在“研究团队”设置研究线、各组人数、Verify 汇报周期与研究总时间。默认配置产生 17 个逻辑 Agent。
3. 检查“计算资源”，创建研究时手动填写任务名称、研究内容、限制以及已有数据/代码目录。可以从按任务名显示的历史记录下拉菜单导入配置，名称单独填写。
4. 在工作台展开成员、选择节点或连线，查看讨论、交接、执行轨迹、实验、证据、记忆和报告。

官方模型服务地址为 `https://api.deepseek.com/anthropic`，DSH 实际请求 Messages 接口。旧配置中的官方根地址或 `/v1` 地址会在调用时兼容转换；自定义代理保留自己的路径前缀。连接测试也使用 Messages 接口，并显式关闭思考以完成最小文本响应检查；通过该测试不代表完整科研流程、工具或流式验收通过。

**真实研究**使用官方完整 SDK profile 和真实模型请求。缺少凭据、数据、依赖或运行能力时会进入阻塞状态，不生成虚构结果。

**演示模式**必须在创建时明确选择。Agent 回应来自标记的预设脚本；CPU 线性回归示例由真实本机进程运行。演示只能验证程序链路，不是题目的科研结论。

API Key 使用当前 Windows 用户的 DPAPI 加密，前端只显示掩码。可选环境变量 `DEEPSEEK_API_KEY` 由后端读取。更换凭据后，已有项目需要在阻塞提示中显式重新绑定凭据版本。不要把密钥写进题目、描述或代码。

## 运行规则

- 同一个群聊每次只有一名成员正式发言。每人私下提交 0–100 的意愿分数，按 KDS 的 softmax((score − 2 × heat) / 1.5) 选择；发言热度每次乘 0.7，当前发言者加 1，有其他合资格成员时不连说。评分与抽签记录持久化，不显示成聊天。各独立研究组仍可并行。
- 提案、审查和投票均覆盖全部配置成员；审查可以修订自己的候选，候选变化后再次审查。未解决的必改项不会进入投票，也不能通过省略而消失；没有实质进展则显示具体阻塞项。
- 消息工具在同一事务保存正文、每位收件成员的收件箱和投递回执，并登记可恢复的通信回复任务。正式模型回合读取固定的收件批次；空闲接收方也会处理消息，回复不会直接形成科研决议或自动循环。旧方向的消息和回复不会进入新方向上下文。
- 问题改类必须提供原分类、复核理由和本研究中已有证据/消息的来源引用；未经校验的改类不能消除阻断。运行监控保存日志游标与固定观察窗口，重试不跳过新日志；已准入且确认清理的启动失败也形成失败包送 Verify。
- 首页可删除已完成、已终止或已隔离的运行记录；在“已删除”中恢复。删除只改变列表可见性，保留消息、证据、文件、检查点与停止记录。进行中的研究先终止再删除。
- 每条研究线固定绑定 Exploration、Critic、Implementation 和 Verify。Boss、Organizer 和 Paper 承担全局职责。
- 仅有效、版本一致的 Verify 报告计轮。同一提交重放不重复计数。第 k 轮无论是否通过都只交 Boss；此前通过交 Boss，未通过同时交本线两个研究组。
- 固定实验方案不可由 Implementation 擅改。实现错误可以修复并保留版本；越界修改返回研究设计流程。
- Organizer 只有具备可启动资源时才授权，任务返回原 Implementation。授权、启动和资源释放受事务、控制代次、进程退出证明约束。
- 只有 Boss 决议可以写共享记忆。报告使用冻结的证据快照，保留负结果、失败和验证边界。
- 唯一预算为整个研究的总运行时间。排队、暂停和重启间隔均计时；各研究线共享原始截止时间。
- “暂停新增工作”等待安全边界，已有实验仍被监控。“立即终止”会关闭启动门禁并停止模型、工具及实验进程树。
- 停止不先等待业务库写入。界面分别显示执行退出、独立停止记录、数据库同步与资源对账状态。
- Control 重启后旧许可作废，原非终态项目被隔离。不能通过恢复命令重新启动已终止或到期研究；继续工作需新建研究。

## 官方 SDK 与工具权限

版本锁定为 `deepseek-harness-sdk==0.2.1rc2` 与匹配的 `deepseek-harness-runtime-bin==0.2.1rc2`。使用完整 `sdk` profile；不以裸模型请求替代 Harness。每个项目/Agent/方向代次保留独立会话，机器级 Organizer 有显式共享身份。

内置 scoped research 插件注册证据读取、共享记忆读取、真实消息发送和轻量 CPU 分析。通用 shell、任意文件写入、未登记子 Agent 等入口被关闭；正式实验统一通过受监督 Runner 启动。轻量计算使用受限数值语法、已登记证据输入及项目总截止时间。该机制负责业务权限和退出控制，不宣称隔离任意恶意本机代码。

创建研究时指定的代码/数据目录可通过受限输入工具分页读取，读取的来源与内容哈希会登记为证据。工具拒绝越出该目录、凭据文件、内部运行数据及逃逸链接；Paper 与 Organizer 不直接浏览输入目录。

如果已经安装了**版本完全匹配**的官方 DSH CLI，可显式配置 `AUTORESEARCH_DSH_BIN` 使用官方 SDK 的公开 `dsh_bin` 参数。适配层会核对版本，不会静默退回其他模型或模拟模式。未设置时使用 SDK 打包运行时。

```powershell
# 可选：仅用于已经存在的匹配版本官方运行时
$env:AUTORESEARCH_DSH_BIN = 'C:/Users/64379/AppData/Roaming/npm/dsh.CMD'
.venv/Scripts/python.exe scripts/probe_dsh.py
```

仅做最小模型连接测试不会证明工具、流式和恢复能力全部通过；SDK 本地协议测试也不等于已通过真实 DeepSeek 服务验收。当前验证记录见 [IMPLEMENTATION.md](IMPLEMENTATION.md)。

## 验证

```powershell
.venv/Scripts/python.exe -m pytest -q --basetemp .test-data/pytest
node --test scripts/test-history.mjs
cd apps/web
npm run build
```

测试包含实际 Windows 进程树、轻量 CPU 分析、数据库写锁时停止、授权竞争、事务故障恢复、DPAPI 密钥隔离、Verify 计轮、Boss 权限、双研究线 CPU 演示及只读历史投影。SDK 协议测试使用本机 HTTP 模型夹具与虚拟凭据，并明确与真实模型验收区分。

历史投影测试使用 Node 22.18+ / 24 内置的 TypeScript 支持。

某些受限制的 Windows 沙箱会阻止 Python asyncio 创建本机 socket pair，表现为异步测试在开始前卡住。应在允许本机回环连接的终端运行测试；不能把这种环境阻塞误当作测试通过。

## 数据与故障处理

`data/business.sqlite` 为业务库；`data/checkpoints.sqlite` 为独立 LangGraph 检查点库；`data/control/` 保留停止墓碑与确认记录；项目产物、会话、日志在各自子目录追加保存。下载仅允许已登记且 SHA-256 一致的产物。

服务日志为 `data/control.log`、`api.log`、`runner.log` 和 `worker.log`。停止日志损坏、旧进程状态不明、数据库故障时，启动门禁保持关闭。可以导出已有记录；不要删除停止墓碑以尝试恢复研究。

备份时先通过启动器停止所有服务，然后一起复制整个数据目录，保留业务库、检查点、停止记录和产物。DPAPI 凭据绑定 Windows 用户，迁移账户或机器后应重新配置密钥。
