# Disaster-Tweets9 运行时发生的问题

## issue 1

删除前端“每个结论，都有可追溯的证据。”、“研究 · 实验 · 证据”等没有用的部分

## issue 2

每个agent/group交流记录界面会有一个下拉菜单来选择会话，每个会话在下拉菜单中显示的内容可读性很差，无法区分识别各自都是哪一轮的，都在干嘛，优化一下这个内容的显示

## issue 3

交流记录默认显示正在运行中的/最新的一次会话，而不是默认显示第一次会话


## issue 4

如果agent发言发生json解析错误，添加一个程序兜底：在末尾加一个}，agent经常犯这样的错误

## issue 5

line1 卡在结束投票的程序校验循环，当前不是实验运行失败。
- Critic 待投票产物只有 content_markdown、config_fingerprint、key_metrics，缺少转入 Implementation 必需的实验计划字段 question、fixed、metrics。
- 第 3 位成员多次正常返回“同意，进入 implementation”，但程序随即校验那份固定产物并报错。
- 自动纠正只让成员重写投票，无法修改被绑定的产物，因此反复出现同一个错误，已累计 21 次技术失败。
- 界面把它统一显示成 local_service_unavailable，掩盖了实际的计划字段缺失。技术失败也不会累计结束投票失败次数，所以无法触发三次失败后的 God 定稿。
问题位置在 [discussion_v2.py 的投票校验 (line 201)](C:/Users/64379/Desktop/Programs/AutoResearch/backend/graphs/discussion_v2.py:201) 和 [实验计划校验 (line 792)](C:/Users/64379/Desktop/Programs/AutoResearch/backend/graphs/engine.py:792)。

是否需要简化交接字段（不只是critic和implementation之间的）？有些不必要字段是否不应该成为阻塞条件？然后重试逻辑也有问题，应该改一下

## issue 6

监控回复缺少必要字段时，应自动触发纠正重试，不应直接导致研究任务阻塞。

### 问题描述

Disaster-Tweets9 的 line2 在实验监控中请求提前停止。Implementation 在正文中说明按 `execution_unusable` 停止，但结构化输出为 `request_early_stop=true`、`rule_id=null`。当时冻结计划的 `early_stop_rules` 为空，程序仍允许使用通用原因 `execution_unusable`，模型却没有将它填入 `rule_id`。

当前字段结构允许 `rule_id` 为空，后续执行校验才拒绝该停止申请，抛出 `invalid_early_stop`（“提前终止意见未绑定冻结规则与真实运行证据”）。该错误直接导致监控所属任务失败，没有把缺失字段交回原 Agent 纠正。实验进程仍继续运行，并最终正常退出，但业务记录未及时同步，留下失败任务和过期的“运行中”状态。

### 解决方案：缺字段自动触发重试

- 保持现有字段设计不变，继续使用 `request_early_stop`、`rule_id` 和说明、证据字段。
- 在接收监控回复时校验条件必填关系：当 `request_early_stop=true` 时，`rule_id` 必须非空，且属于冻结规则编号或通用原因 `execution_unusable`；继续运行的回复不强制填写停止规则编号。
- 缺字段、字段为空或规则编号无效时，不执行停止，也不直接将研究任务判为失败或阻塞；自动向原监控 Agent 发起纠正重试。明确返回出错字段、原值、当前合法编号及补填要求，要求重新返回完整 JSON。不得由程序根据正文猜测并代填停止原因。
- 纠正沿用同一监控回合、固定日志窗口和证据，不提前推进监控游标，不重复启动实验。实验继续受既有 Runner 和停止控制监督；用户停止、项目截止时间等控制始终优先。
- 同一监控回合的生成纠错不设次数上限，尝试、原始回复和字段错误持久保存，重启后复用回执而不重复执行。只有字段和原有停止条件校验全部通过后，才接收决定并推进游标；停止申请通过后才执行停止。
- 纠正期间继续跟踪并同步实验真实终态，实验退出后停止该窗口的纠正；不因这类回复错误遗留无人跟踪的运行记录，也不将纠正失败伪装成有效的继续或停止决定。

### 验收要点

- `request_early_stop=true`、`rule_id=null` 或缺少 `rule_id` 时，自动纠正重试，首次错误不使研究任务阻塞。
- 冻结规则列表为空时，纠正提示仍列出合法通用原因 `execution_unusable`；不接受模型虚构的规则编号。
- 模型补齐字段并通过原有校验后，同一监控回合只接收一次有效决定，不重复发起停止操作。
- 纠正期间及重试耗尽后，实验退出状态均能同步，失败与重试记录可追溯。

## issue 7

简化verify输出字段，evidence直接写在报告里，不需要程序验证。independent_replication、execution_checks、required_changes、limitations字段都可以在报告里写出来，属于冗余字段，取消。

## issue 8

加一个角色：watch dog，在所有运行agent之外，固定每n分钟运行一次（可以在高级配置里面配置，默认为10），每次运行检查：
有没有哪一条研究先没有任何步骤正在运行（共享部分organizer和boss在运行也算在运行），这个检查由程序实现，如果有阻塞情况才启动agent解决
agent先分析当前状况，然后可以通过提示词注入（提示agent哪里有问题，应该怎么改）或者其他方式尝试在线修复。watch dog权限很高，在线修复方法很多，不要做太多限制。

## issue 9

Implementation 为正式实验申请的主机内存不足，导致实验因内存耗尽（OOM）失败时，系统应自动调高内存预算，重新申请资源，并用同一份代码和实验配置重试。每次尝试应保留原运行记录和实际预算。达到可分配内存上限或重试次数上限后，记录失败原因并结束自动扩容。
