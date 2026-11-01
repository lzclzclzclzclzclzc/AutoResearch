# Disaster-Tweets5 / 6 仲裁运行核查

核查日期：2026-10-30。只读查询 `data/business.sqlite`，核对项目、仲裁请求、各次尝试、God 原始输出、冻结现场与后续依赖事件，没有重试、恢复或改写项目。原始两次研究均发生在 2026-10-29，且最终由用户停止。

## 统计口径

一次仲裁请求可以生成多份回复。下表“采用”取请求的实际 `applied` 状态；“拒绝”取每次尝试保存的诊断，不把模型生成完成当成判决被采用。

| 项目 | 仲裁请求 | 已采用请求 | 重试耗尽请求 | God 回复尝试 | 被拒回复 |
|---|---:|---:|---:|---:|---:|
| Disaster-Tweets5 | 5 | 2 | 3 | 11 | 9 |
| Disaster-Tweets6 | 2 | 1 | 1 | 5 | 4 |
| 合计 | 7 | 3 | 4 | 16 | 13 |

被拒绝的 13 次尝试，按程序最先报告的错误分类（同一回复还可能包含后续未检查到的问题）：

| 直接拒绝原因 | 次数 |
|---|---:|
| Recovery cannot expand the original role permissions：目标角色不被允许 | 5 |
| God selection cannot remove unresolved blockers：存在未解决阻断项，禁止选方案 | 4 |
| Dependencies must identify other existing research lines：依赖填了本研究线 | 2 |
| God evidence reference is outside the frozen authorized snapshot：引用未匹配快照引用集合 | 1 |
| extra_forbidden：多返回 action_note 说明字段 | 1 |

## 关键案例：Disaster-Tweets6 / line-02

请求：`arb-98f164abcda96591b33c6299290834dc48c4323b`。讨论为 Critic，第 9 轮，只有一个成员。

被冻结的阻断项包括：基线接口缺失；遇 train/test 同文即硬停；遇跨折重复文本即硬停；两个结果文件没有写出代码、判定函数未被调用；诊断输出列缺失；折分配校验不足；NB-SVM 广播方式问题。部分属于真实代码/设计问题，不能一概认定为无意义检查。

God 的三次动作及结果（北京时间 2026-10-29）：

1. 20:18:08，`request_evidence`，target_role=`implementation`，dependency_line_ids=`[line-01]`：希望取得基线、折划分等内部产物。被“不得扩张原角色权限”拒绝。
2. 20:20:00，`request_revision`，target_role=`exploration`：明确要求修改原方案作者 Exploration 的 rev2 产物，修复上述代码/设计问题。再次被“不得扩张原角色权限”拒绝。
3. 20:22:02，`select_candidate`：前两条路径被封后，God 改选已给出修复设计的 Critic 候选，同时保留原阻断项和未验证状态。被“不能消除未解决阻断项”拒绝，三次预算耗尽。

God 第三次原文明确解释：前两次取证和修订被权限规则关闭，内部缺件又不适合要求用户补充，结束研究也没有可复用结果，因而尝试选择现有候选继续推进。

判断：不能简单认定第三次应无条件放行，原对象确实有缺陷。但第二次“让 Exploration 修改自己的方案”是合理的恢复路径；程序当时把任务转交其他角色等同于 God 自己取得其他角色权限，造成了恢复路径缺口。

## 关键案例：Disaster-Tweets5 / line-01

请求 `arb-4ad06548a5d825da9a818cc503ad1ffe5d5432b7` 与后续请求 `arb-162ca67d95ac6633d0e8285f3e7bb44934e702b6` 处于同一恢复链。

God 反复指出：方案已有规范化脚本、折生成脚本、评测脚本和容差计算式，尚缺的是实际运行得到的泄漏上界、重复率、基线 F1/标准差、OOF 和产物哈希。这些应在运行后验收，却被当成设计阶段放行条件。

实际形成循环：

    想运行，产生真实统计与产物
    → 设计尚未放行，不能开始正式实验
    → God 尝试安排取证/选定设计
    → 本线依赖、跨角色或未解决阻断项规则拒绝
    → 回到必须先有运行产物才能放行

其中两次 dependency_line_ids 填了本线 line-01，这确实不符合“跨线依赖”字段的含义。删除自依赖保护会让本线等待自己，不能解决根因；需要可执行的本线运行/执行阶段验收路径。

后续请求第一次在北京时间 17:49:01 被拒的唯一已报告错误，是多写了 `action_note` 说明字段。这是明确的多余格式限制，上一轮“提取所需字段、容忍额外字段”修改已覆盖。放宽这一项本身不保证随后能通过阻断项规则。

## 已采用的仲裁为什么仍未推进实验

- Tweets5 的 line-03 取证请求已采用，随后 Boss 的平票仲裁也已采用；line-03 进入等待 line-01/02 交付依赖的状态。但两条上游线均陷入阻塞，没有产生所需产物。
- Tweets6 的 line-03 取证请求第二次已采用。第一次 target_role=implementation 被拒，改成 exploration 后，核心取证内容基本保留而获准。之后 Boss 协调任务在北京时间 21:42:31 因 `model_execution_failed` 失败；line-01 此前 20:58:43 也有同类失败。
- 数据库中 Tweets5 没有冻结的 ExperimentSpec；Tweets6 只有 1 份 ExperimentSpec。两项目均无 Implementation、正式运行、提交或验证报告记录。因此，“判决已采用”只代表协调动作启动，不代表依赖交付或正式实验已完成。

## 对当前代码的判断

确有过强约束和恢复路径不足，但并非所有拒绝都应取消：

- 已覆盖：容忍 action_note 等额外说明字段。
- 当前工作区已有改进：Critic 僵局可沿登记路径请求同线 Exploration 修订；问题可保留身份并转为执行阶段验收。
- 仍存在：God 不能直接处置已有阻断项；跨角色取证仍受原角色约束；路由/字段问题与实质判决失败共同消耗三次尝试预算。
- 不能仅删除自依赖校验、引用校验或直接选择闸门来解决：可能掩盖真实缺陷、产生等待环，或者把没有运行过的结果当成完成。

建议的后续方向是赋予 God 明确的流程裁决能力：给已有问题记录“维持设计阻断 / 移交执行验收 / 交原作者修订 / 有依据判定为非阻断”等处置；允许向正确角色派发任务，执行仍使用该角色自己的身份；由程序处理内部路由和引用映射。这样 God 可以改变导致僵局的状态，而不是只能在不能执行的几个动作之间换一种写法。

## 证据定位

- Tweets5：`research-b4da2960172346899f748340f559fb9c`
- Tweets6：`research-d5d5458d53f74347a250bb568f415513`
- 逐请求、逐尝试诊断与事件提取：`.test-data/disaster-arbitration-audit.json`
- 相关 God 输出和阻断项摘录：`.test-data/disaster-arbitration-details.txt`
- 关键当前规则：`backend/graphs/arbitration.py` 的 validate_decision / revision_roles；`backend/graphs/dependencies.py` 的 line_ids。
