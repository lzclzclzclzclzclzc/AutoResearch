# Disaster-Tweets5 运行检查

检查日期：2026-10-29。以下时间均为北京时间（UTC+8）。

研究 ID：`research-b4da2960172346899f748340f559fb9c`。

检查依据为只读业务数据库中的项目、任务、事件、仲裁快照、模型执行、消息和工具调用，并对照当前代码。没有修改历史记录、服务配置或执行逻辑。

## 1. 三条线为什么停止

结论：研究在实验设计阶段死锁，1、2 线自动仲裁耗尽，3 线等待它们的产物，随后项目被判定为全部阻塞。最后的终止是另外收到的用户停止请求。

本次没有生成正式 spec、Implementation、run、submission 或 verification 记录；三线有效 Verify 轮数均为 0。不是实验已经完成，也没有耗尽总时间：配置为 360000 秒（100 小时），原截止时间为 11 月 2 日 21:12:39。

| 时间 | 事实 | 事件序号 |
| --- | --- | --- |
| 17:12:39 | 研究启动 | 以 research.started 记录为准 |
| 17:18:12 | Boss 分派三线，同时派发第一份 Paper | boss.tasks_materialized |
| 17:25:41 | 3 线进入等待 1、2 线产物的状态 | 2561 |
| 17:35:16 | 2 线 God 三次尝试耗尽 | 4150 |
| 17:46:27 | 1 线 God 三次尝试耗尽 | 4709 |
| 17:46:27 | 所有未完成研究线均阻塞 | 4712 |
| 17:47:23 | 收到 retry 命令，重新恢复研究 | 4713；command kind=retry |
| 17:51:41 | 1 线新增仲裁仍耗尽，再次全部阻塞 | 4886 |
| 17:51:49 | 按 user_requested 终止 | 4889 |

### 1 线：把运行后的结果变成设计放行条件

Critic 第 10 轮冻结现场有三个 unresolved must_fix：`MF-01`、`MF-06`、`issue-shared-folds-leakage`。

- `MF-01` 的 resolution 已写“设计已闭合”，但因“泄漏上界未实测”仍保留 resolved=false。
- `MF-06` 要求先有 train/test 统计、基线 F1/std、产物 SHA256 与生成时间，并明确说“须由 Runner 实跑 stats.json 回填”。
- 重复文本泄漏项仍因重复率没有实测而阻塞。

这些结果需要先通过设计门禁、进入实现和 Runner 才能产生。当前审查却等待它们产生后才放行，构成循环依赖。程序拒绝直接绕过 unresolved blocker 的校验本身符合当前合同；问题在于设计缺陷和运行后验收条件没有被有效分开。

God 尝试取证时先填了依赖本线 `line-01`，被自依赖检查拒绝；后来把 target_role 指向 exploration，又与原 critic 角色不符；改成选候选则因 blocker 未解决被拒绝。重试后的新仲裁还出现额外字段 `action_note`，最终又落回自依赖。

### 2 线：等待共享协议，恢复请求的角色不匹配

Critic 第 8 轮仍有 `issue-near-dup-label-conflict`：需要 1 线的规范化规则、冲突组清单、共享折文件等。God 判断应向 1 线取证，但连续三次输出分别因以下原因拒绝：

1. evidence_refs 含冻结授权集合外的引用。
2. 当前场景角色是 critic，target_role 却为 exploration。
3. 转而选择候选时，未解决的 blocker 仍在。

### 3 线：真实等待上游，没有可消费的结果

God 已采用 request_evidence，依赖是 `line-01` 和 `line-02`，缺少共享 folds、评测脚本、基线容差和 OOF。Boss 后续协调时仍没有 submission/verification，只发布了更详细的交付要求，没有产生依赖结果。上游不运行，3 线就无法被唤醒。

### 加剧死锁的工具问题

13 次 run_light_cpu 调用中，12 次失败。只把这些原始代码重新送入本地语法校验器（没有执行计算），12 次均可复现拒绝：

- 7 次：调用不在允许表中的函数，如 type/isinstance/hasattr。
- 3 次：导入不允许的模块，如 io/re/hashlib。
- 2 次：使用不允许的对象属性/方法，如 dict.items。

工具把具体原因统一显示成“科研工具请求未获授权或参数无效”。成员因此误以为没有计算权限，多轮改写仍没有正确使用工具，也把这个失败继续作为研究阻断理由。

### 重试为什么没有解决

retry 没有引入新证据或修订原来的设计 blocker。2 线重新遇到同一个已耗尽请求。1 线形成了新请求，但前后两个仲裁快照的顶层内容只有 dependency_catalog 不同，科学候选、问题和讨论均相同；目录中阶段变化导致重新判决，未带来新的科学进展。

代码位置：[设计讨论门禁](../backend/graphs/discussion.py)、[God 校验与三次/600 秒限制](../backend/graphs/arbitration.py)、[跨线依赖检查](../backend/graphs/dependencies.py)、[全部阻塞判断](../backend/persistence/runtime_recovery.py)、[轻量计算语法限制](../backend/runtime/light_cpu.py)。

## 2. Paper 的触发时机

当前 Paper 是阶段报告，最终论文另设流程，见 [design.md](design.md)。

具体触发条件是 Boss 决策的 `request_report`：

- `BossDecision.request_report` 默认值为 true。
- Boss 决策确认后，apply_boss_effects 只要看到 request_report=true，就冻结当时材料并添加 paper 任务。
- 没有“至少完成一次实验/Verify”的前提，也没有定时、进展增量或报告去重门槛。
- k=2 控制 Verify 向 Boss 汇报的周期，不直接控制 Paper；初始 Boss 和内部依赖协调也能派 Paper。
- Paper 可以和三条研究线并行。立即终止只封存已有材料，不额外启动模型补写报告。

本次两个 Boss 决策都为 request_report=true：

| 报告 | 派发 | 完成 | 冻结内容 |
| --- | --- | --- | --- |
| 初始分派报告 | 17:18:12 | 17:18:48 | assign，submission=null，cycle_history=[] |
| 内部依赖协调报告 | 17:33:28 | 17:34:02 | request_evidence，submission=null，cycle_history=[] |

因此两份都是无实验结果的协议/进展报告，并非完成研究后的论文。

另有证据打包缺陷：两个 report_snapshot.evidence_ids 均为 []，按适配器的 artifact_id 字段收集算法，两个回合的 permitted_artifact_ids 也均为 []。Boss 只在 memory_updates.source 等字符串中提及原始 artifact，没有将其作为可读取的冻结附件传入。Paper 的 read_research_evidence 有 5 次失败，其中还把一个依赖任务 ID 当成 artifact_id。不能通过放开所有附件读取来补救，应正确冻结并显式授权报告所需的真实证据。

代码位置：[默认触发字段](../packages/contracts/research.py)、[任务创建与证据快照](../backend/persistence/store.py)、[Paper 执行](../backend/graphs/engine.py)、[附件授权收集](../backend/adapters/dsh.py)、[读取权限](../backend/adapters/gateway.py)。

## 3. God 活动记录为什么显得无效

### 实际无效输出

共 5 个仲裁请求、11 次模型尝试，仅 2 个裁决通过校验并应用：3 线等待内部依赖、Boss 等价候选打破 1:1 僵局。其余 9 次输出没有成为有效裁决。后者中 8 次只是模型执行完成，随后业务校验失败；1 次直接未通过 JSON 合同校验。

“模型生成完成”和“裁决已采用”是两个不同事实。God 原始消息在业务校验之前就写入，界面会展示那些最终被拒绝的长篇裁决文字；消息的 complete 状态没有说明其业务校验结果。

### 确认存在的重复消息

God 独立会话共 23 条消息，其中 10 份正文各出现两次，另有 3 条单独内容。

重复来源是一份 DSH 流式消息（source_system=dsh）加一份 execute_god_turn 创建的最终消息（source_system=god_generation）。两份 message_id 不同，前端按 message_id 去重无法合并。应复用原消息身份，并补入最终结构化结果与验收状态。

### 确认存在的容量等待噪声

11 次调用产生 33 条 arbitration.capacity_wait 与 11 条 capacity_acquired。每次调用连续报三条等待，对应三个门禁。

从每次首条等待到 acquired 仅 47.3–156.9 毫秒。_admission 为每个 gate 创建异步 acquire 任务，任务尚未得到运行机会时即进入等待分支；随后即便 acquire 已完成，也没有在 emit 前重新检查。announced 又在每个 gate 内重置，导致无真实拥塞时也可能连报三次等待。这不能作为模型容量不足的证据。

代码位置：[提前发布 God 消息](../backend/graphs/engine.py)、[容量门禁事件](../backend/adapters/dsh.py)、[聊天按消息 ID 合并](../apps/web/src/components/Chat.tsx)、[关键活动筛选](../backend/persistence/event_views.py)。

## 建议修正顺序

1. 先修设计与执行的循环依赖：设计 gate 验收协议是否完整、可执行；统计量、OOF、F1 和产物哈希进入运行后验收。保留原问题及依据，通过合法修订/改类推进，不直接抹掉 must_fix。先让 1 线产出共享协议和基线，再唤醒下游。
2. 给每个 God 请求明确传入可用 target_role、合法依赖线（排除本线）与动作前提；校验反馈指出具体字段、非法值和允许值。原角色的 request_revision 是可用恢复路径，应引导成员修正问题分类和验收阶段。
3. 修轻量计算的能力说明和错误反馈，或提供安全的 CSV/文本统计入口，使科研成员能执行常见数据分析。
4. Paper 默认不随初始分派/纯协调触发；在有新实验及 Verify 结果、阶段结束或明确请求报告时触发，并正确冻结、授权证据。是否保留无实验的阶段报告可作为产品选项。
5. 合并 God 流式与最终消息；把未通过校验的尝试明确标为“未采用”，默认收起原文；关键活动只突出裁决应用、恢复结果及失败原因。
6. 容量等待只在确有等待且超过展示阈值时发一次；acquired 仅用于结束已展示的等待。不要用阶段目录的瞬时变化触发相同科学现场的重复仲裁。

以上为检查结论和修改建议，本次未实施修复。

## 后续修复记录

在后续修复请求中已完成以下代码调整：

- 设计门禁与运行后验收分开。运行后条件仍保持原 issue_id、must_fix 分类和未解决状态；调整验收阶段需要有真实来源的 stage_review。组内总结遗漏时，程序保留成员审查中的执行验收条件，冻结计划必须继续携带，Verify 逐项引用真实产物核查后才允许通过。
- God 输入明确本次合法角色、可依赖的其他研究线及动作前提；拒绝反馈给出具体字段与允许值。显式重试已耗尽仲裁时建立新恢复版本，不重复返回同一耗尽现场；瞬时研究线阶段不再单独改变科学现场。
- God 流式与最终消息复用身份，校验结果与原因单独记录并支持按事件游标回放。旧重复消息在显示层配对合并，未采用的尝试默认折叠。真实容量等待超过短暂阈值才显示，同一调用只提示一次。
- Paper 的时机保持由 Boss 决定；没有改变 request_report 默认值或引入“必须有实验才能报告”的策略。修复冻结材料中真实附件的收集与读取授权，跨项目附件继续排除。
- 兼容另一条聊天已提交的普通 Python 轻量计算实现，保持其工作区、资源及停止控制；不恢复旧白名单。项目 NumPy/Pandas 依赖已核对安装。
- 旧计划与 Verify 输出在未使用新增字段时保持原有序列化结果，避免改变已有冻结内容的哈希。

验证覆盖 401 个后端用例：全量运行 396 项通过，5 项因测试目录使目标路径达到 276 个字符而失败；换短目录后这五项及同组额外一项均通过。15 项前端消息、回放与流式测试通过，前端构建通过；实际历史页面在桌面及手机宽度下无横向溢出或页面错误。

修复期间没有向研究发送停止或重启指令，没有改写历史运行数据，也没有重启后端服务。最后只读检查发现 Disaster-Tweets6 已于北京时间 22:01:11 以 user_requested 原因终止。后端代码变更在后续服务重启时加载。
