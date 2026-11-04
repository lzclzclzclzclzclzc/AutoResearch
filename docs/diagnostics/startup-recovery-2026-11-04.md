# Disaster-Tweets10 启动故障与恢复

日期：2026-11-04，Asia/Shanghai。

研究 `research-44bc7279bf584455ac8e2ed819481235` 于 19:00 创建，
supervisor 任务一直 pending、attempt=0，没有模型执行记录。
用户于 19:28 发出的终止指令已在独立控制服务持久保存，但业务库仍显示 created。

## 原因

- Runner 启动时同步对账历史执行，初次观察中约 80 秒才完成 876 条执行
  记录重放。启动器只等待 25 秒，超时后跳过 Worker；原 WorkerWatchdog
  对从未创建的 Worker 直接返回，因此没有后续调度和终止同步。
- `data/control/line01-plan-repair-20261031.json` 是没有日志包装与校验字段的
  人工修复备份。控制服务按损坏日志处理，设置 journal_unknown 并关闭新研究门禁。
- 健康接口只检查数据库和控制通道是否可连接，未检查 Worker 心跳或控制日志
  完整性。Worker 心跳停在 11 月 1 日 18:38，接口仍返回 ok。

## 修复

- 保留初次有界就绪检查，随后继续带退避地探测 Runner；Runner 就绪后自动
  补启缺失的 Worker。保持已有 Worker 的唯一进程句柄和独占调度锁。
- 健康接口检查运行状态及 30 秒内的心跳，缺失、无效、过期或明显未来的
  时间戳均不可用；控制日志未确认时顶层状态为 degraded。
- 校验备份 SHA-256 后，将唯一已确认的非日志文件移至
  `data/diagnostics/control-backups/line01-plan-repair-20261031.json`。
  控制日志读取逻辑和真实损坏日志的门禁保护保持有效。
- 确认没有在途研究、没有未退出执行后，只重启身份已核对的本安装进程树。
  原停止记录、研究数据及备份完整保留。

## 验收

三个相关测试模块共 64 项通过，包括 80 秒延迟启动、缺失 Worker 补启、
健康检查及归档后继续保留旧停止决议。独立代码审查未发现需要修改的问题。
完整 Python 测试集 **726 passed**，5 条依赖层警告，用时 1018.44 秒，
退出码 0；日志为 `.test-data/startup-full.log`。

19:57 本机验收确认：Worker 自动补启且心跳持续前进；API 顶层为 ok；
Control journal_unknown=false；Runner 可用；14 个原 stop_id 完整保留。
Disaster-Tweets10 的数据库与控制状态均为 terminated，supervisor 为
cancelled、attempt=0，模型执行数为 0。未创建或重跑用户研究。

机器验收记录位于 `.test-data/startup-repair-before.json`、
`.test-data/startup-repair-after.json`；备份 SHA-256 为
`0933a409d5eae15e7054d51c2fd6eb8f8d08dfd2a33b891237edf487ecb4e135`。
