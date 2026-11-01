# 自动化 ML Research Team：相关论文与开源系统综述

整理日期：2026-10-24。用途：为 AutoResearch 的角色分工、实验闭环和共享记忆设计提供文献依据。

本文整理此前讨论过的相关工作，重点分析 2026 年的 **The Agentic Scientist、AutoScientists、EvoScientist**，以 2024—2025 年的系统解释方法演进。它是一份面向当前设计的定向综述，不是覆盖全部领域的系统性文献检索，也不构成系统性能排行榜。

论文日期以预印本首次提交为主，修订日期、正式发表日期另列；开源实现以本次查阅的仓库文档为准。默认分支会继续变化，因此下文的仓库描述不等于论文实验使用的固定版本。本文不把项目宣传、论文中的实验结果和我们自己的设计推论混为一谈。

## 1. 核心认识

从这些工作中可以归纳出三类互补的研究方向：

1. **把一次研究做完整**：明确问题与评价条件，让想法、实现、实验、证据和报告连起来。The AI Scientist 系列和 AI-Researcher 主要提供这类参考。
2. **让多条研究线持续协作**：并行探索、共享实验反馈、分配稀缺算力、在停滞时调整方向。AutoScientists 是重点参考。
3. **让经验在下一次研究中继续起作用**：从成功和失败中提炼可检索的知识，而不是只保留聊天记录。EvoScientist 是重点参考。

这是本文的分析框架，并非各论文采用的统一分类。对 AutoResearch 来说，关键不是增加多少 Agent，而是保证每个角色拥有明确的决策对象：研究组决定“试什么”，Implementation 决定“如何实现既定方案”，Organizer 决定“何时、用什么资源运行”，Verify 判断“现有证据支持什么”，Boss 决定“下一步投入哪里”。

## 2. 时间线：哪些是当前重点，哪些只是背景

| 工作 | 首次公开论文 | 本次核对的修订或发表 | 对本文的主要价值 |
|---|---|---|---|
| The AI Scientist | 2024-08-12 | arXiv v3：2024-09-01 | 端到端自动科研的早期基线 |
| Virtual Lab | bioRxiv：2024-11-12 | Nature 在线发表：2025-07-29 | PI、专家和 Critic 的会议协作 |
| Agent Laboratory | 2025-01-08 | arXiv v2：2025-06-17 | 人类反馈下的科研流水线 |
| Co-Scientist | 2025-02-26 | arXiv v2：2026-06-29，标题更新 | 假设生成、评审、排序与迭代 |
| AgentRxiv | 2025-03-23 | 本文参考 v1 | 通过研究报告跨团队积累知识 |
| The AI Scientist-v2 | 2025-04-10 | 本文参考 v1 | 实验树搜索与实验管理 |
| R&D-Agent | 2025-05-20 | arXiv v2：2025-10-01，标题更新 | Research / Development 分工与反馈 |
| AI-Researcher | 2025-05-24 | 本文参考 v1 | 从文献到实现、实验和论文的组织 |
| **EvoScientist** | **2026-03-09** | **arXiv v2：2026-10-05** | **研究经验与工程经验的持续积累** |
| **AutoScientists** | **2026-05-27** | 本文参考 v1 | **长期实验、多团队协作和资源执行分工** |
| **The Agentic Scientist** | **2026-10-13** | 本文参考 v1 | **研究契约、编码 Agent 编排与证据闭环** |

日期来源：各工作的论文版本历史，链接见第 9 节。Virtual Lab 的 DOI 中包含 `2024.11.11`，但预印本页面标注的发布日是 **11 月 12 日**；AutoScientists 的实验室公告发表于 5 月 28 日，晚于预印本提交一天。项目发布公告与论文首次提交不是同一日期。[Virtual Lab 预印本](https://www.biorxiv.org/content/10.1101/2024.11.11.623004v1)；[AutoScientists 论文](https://arxiv.org/abs/2605.28655)；[实验室公告](https://zitniklab.hms.harvard.edu/2026/05/28/AutoScientists/)

`karpathy/autoresearch` 作为仓库型工程参考单独讨论；本次未确认与之对应的正式研究论文，因此不为它编造论文提出日期。

旧工作适合回答“这个思路从哪里来”，近期论文和当前实现适合回答“今天值得怎样做”。同时，Co-Scientist 首发于 2025 年，但 2026 年仍有修订；不能仅按首发年份直接排除。

## 3. 三个重点系统

### 3.1 The Agentic Scientist：围绕研究契约组织编码 Agent

**论文**：Robert Tjarko Lange 等，*The Agentic Scientist: Orchestrating Coding Agents for Automated Scientific (Co-)Discovery*，2026-10-13。对应开源项目为 `SakanaAI/AI-Scientist-v3`。

**论文方法。** 系统利用编码 Agent 执行研究，在外层组织任务定义、想法生成、实验尝试和后续分析。值得关注的是：先约定任务与评价要求，再依据实验结果判断；指标提升与其他研究要求分开检查；研究历史经过提炼后供后续尝试使用。其研究产物还包括消融、分析和论文，而非只有一份最优代码。[论文方法与评估](https://arxiv.org/html/2610.18933v1)

**当前仓库。** 文档把默认流程分为任务起草、调查实验、消融、最终评价、事后分析、论文写作六阶段，最终稿审阅需要显式启用。需要特别注意：阶段名称不代表每一阶段都是强制通过条件。部分后续阶段失败会标记为降级，流程仍可继续；具体任务契约可以要求必须完成最终评价。[流水线文档](https://github.com/SakanaAI/AI-Scientist-v3/blob/main/docs/pipeline.md)

并行工作者拥有各自的运行记录，可以同步记忆和结论。仓库提供关闭这种同步的配置，适合研究共享记忆的实际作用；它不等于让所有 Agent 直接编辑同一份运行中的代码。[使用与并行运行文档](https://github.com/SakanaAI/AI-Scientist-v3/blob/main/docs/usage.md)

```text
任务与评价约定
    ↓
提出想法 → 编码与实验 → 评价、整理经验
    ↑                        │
    └──── 后续尝试读取经验 ────┘
    ↓
补充证据 → 分析 → 写作
```

图示是本文对流程的简化，不是仓库的完整执行图。

**证据应怎样理解。** 作者提供了不同任务、模型和编码框架的实验，也报告了与人类共同完成研究的案例。这些结果支持“编码 Agent 可以承担越来越多研究步骤”，但不能直接解释成任意任务都能无人干预地产出可靠论文；自动评审也只是评价工具之一。[作者项目说明](https://pub.sakana.ai/agentic-scientist/)

**对 AutoResearch 的启发——本文推论。** 最有价值的是把 Experiment Spec 当作贯穿全程的约定：运行前固定问题和比较条件，运行后追加实际配置、结果、验证及决策。这样 Verify 评估的是具体版本中的证据关系，Paper Agent 也有明确的报告依据。我们不必照搬其最终论文流水线，更不能因为仓库允许某阶段降级，就让 AutoResearch 把缺失证据视为已经通过验证。

### 3.2 AutoScientists：围绕假设组织团队，持续产生和执行实验

**论文**：Shanghua Gao、Ada Fang、Marinka Zitnik，*AutoScientists: Self-Organizing Agent Teams for Long-Running Scientific Experimentation*，2026-05-27。

**论文方法。** 多个长期运行的 Agent 围绕研究假设组成团队，通过共享论坛交流提案和结果，并在探索过程中调整团队和方向。它强调并行保留不同研究路线，而不是要求全体讨论后只能执行一个共同方案。论文在多类实验任务中评估系统，并对分析角色、反馈和组织方式进行消融。[论文](https://arxiv.org/html/2605.28655v1)

**四个部分如何配合。** 以下主要解释当前开源文档中的执行机制；任务配置会改变具体策略。

| 部分 | 核心职责 | 与其他部分的交接 |
|---|---|---|
| 运行协调器 Orchestrator | 启动或重启 Agent，按任务配置安排资源、检查运行状态 | 让合适的执行者有机会工作，本身不训练模型 |
| 团队分析 Agent / Analyst | 阅读证据、研究机制、提出下一步实验 | 把提案正式登记到共享讨论和团队队列 |
| 实验队列 | 保存待执行实验及其状态 | 让执行者知道哪些工作待做、哪些已被占用 |
| 实验 Agent / GPU Agent | 取得任务、实现与运行、记录结果 | 发布结果，更新相关实验状态，供后续分析使用 |

运行协调器的职责依据 [runbook](https://github.com/mims-harvard/AutoScientists/blob/main/runbook.md)；提案提交依据 [Analyst 协议](https://github.com/mims-harvard/AutoScientists/blob/main/system/templates/ROLE-ANALYST.md)；认领与结果回写依据 [GPU Agent 协议](https://github.com/mims-harvard/AutoScientists/blob/main/system/templates/ROLE-GPU.md)。队列本身是工作状态，不是第四个负责科学推理的 Agent。

```text
Analyst 分析已有证据
    ↓
登记实验提案 → 团队实验队列
                    ↓
             GPU Agent 认领并执行
                    ↓
            结果、失败原因、共享讨论
                    ↓
              后续提案或方向调整

Orchestrator：在外层安排 Agent 的运行与资源
```

例如，分析者建议比较两种训练策略，登记其假设和设置；执行者取到这项工作后写代码、训练并上传结果；分析者再据此提出下一项对照。论文所说的“自组织”主要描述研究活动和团队形成方式，不能据此推断实现中没有运行协调器。

**实现边界。** GPU Agent 的协议也允许在特定情况下自拟实验，但仍要求登记完整轨迹。因此它不是绝对的“分析者只能想、执行者永远不能想”。另外，基础运行手册通过 task profile 决定启动、停止和资源策略，不宜把某个任务的队列规则推广成全部任务都相同。[GPU Agent 协议](https://github.com/mims-harvard/AutoScientists/blob/main/system/templates/ROLE-GPU.md)；[运行手册](https://github.com/mims-harvard/AutoScientists/blob/main/runbook.md)

**证据应怎样理解。** 作者报告的任务表现和消融结果，为其测试条件下的协作价值提供了依据；不能从中推出团队人数越多越好，或者队列认领一定优于固定执行者。后者需要在相同任务与资源条件下另行比较。[作者的评估介绍](https://autoscientists.openscientist.ai/)

**对 AutoResearch 的启发——本文推论。** 可借鉴“科学提案和运行资源分开决定”“完整记录待做、已做、失败的实验”“用实验反馈驱动下一轮”。当前设计已经有意改变认领方式：代码由 `Implementation_i` 编写，Organizer 批准后仍交回它执行。这样保留实现上下文，并让代码修复与运行监控有同一个责任人；这是我们的组织选择，不是 AutoScientists 已经证明更优的方案。

### 3.3 EvoScientist：让研究经验和工程经验持续发挥作用

**论文**：Yougang Lyu 等，*EvoScientist: Towards Multi-Agent Evolving AI Scientists for End-to-End Scientific Discovery*，首发 2026-03-09，本文参考 2026-10-05 的 v2。

**论文方法。** 三个逻辑角色分别是 Researcher Agent（RA）、Engineer Agent（EA）和 Evolution Manager Agent（EMA）。RA 生成与改进研究方案；EA 搜索实现、运行实验；EMA 把历史轨迹提炼成两类持久记忆：研究方向与想法验证经验，以及数据处理、训练和实现策略。后续任务检索这些经验，再用于生成方案和执行实验。其“演化”主要体现在经验对行为的持续影响，不能理解为每做一次实验就更新基础模型权重。[论文方法](https://arxiv.org/html/2603.08127v2)

```text
研究记忆 → RA：想法与方案搜索
                     ↓
工程记忆 → EA：实现与实验搜索
                     ↓
              历史轨迹与结果
                     ↓
              EMA 提炼可用经验
                  ↙       ↘
              研究记忆   工程记忆
```

**当前仓库不是论文角色图的简单复刻。** 项目 README 列出 plan、research、code、debug、analyze、write 六种子角色，并描述跨会话记忆、自动提炼及知识关联。这是当前产品实现的分工；论文中的 RA / EA / EMA 是研究方法层面的划分。引用时应说明自己参考哪一层。[当前开源 README](https://github.com/EvoScientist/EvoScientist/blob/main/README.md)

**证据应怎样理解。** 论文评价了想法质量、代码执行及端到端研究，并报告特定会议 AI Scientist Track 的接收结果。不同评价回答不同问题：想法评分反映评审意见，执行成功反映能否完成运行，接收结果与特定场合的评审标准相关；它们都不能单独证明一般科研结论正确。[论文实验](https://arxiv.org/abs/2603.08127)

**对 AutoResearch 的启发——本文推论。** 共享记忆应该区分“某种科学判断在什么条件下成立”和“某个执行问题如何解决”。前者进入研究分支，后者进入工程分支。条目还应保留适用条件、证据来源和状态，避免把局部成功变成通用规则，或把一次实现失败误记为科学假设失败。

当前设计采用 Boss 唯一维护、树形索引、其他角色按需读取。这些是结合本项目权限和上下文成本作出的选择；论文的双记忆机制为分类提供启发，但没有证明“Boss 单写入 + 树形组织”是最优形态。

## 4. 早期与补充工作：保留哪些参考价值

### 4.1 The AI Scientist 与 The AI Scientist-v2

2024 年的 The AI Scientist 展示了从想法、实验到论文与评审的自动化流程，依赖预先准备的实验模板。它适合作为历史起点。2025 年的 v2 更强调实验管理和树搜索，并降低对人工代码模板的依赖。两者帮助我们理解：一个方向并不只有“成功或放弃”，还可以保留多个实现与实验分支。[v1 论文](https://arxiv.org/abs/2408.06292)；[v2 论文](https://arxiv.org/abs/2504.08066)

v2 的 workshop 级研究案例不应被简写成“已经普遍实现顶会级自主科研”。对本项目更有用的借鉴是保存搜索历史及选择理由，而非直接采用旧版提示词或固定软件栈。

### 4.2 Agent Laboratory 与 AI-Researcher

Agent Laboratory 将文献综述、实验和报告组织成可接受人类反馈的研究流程；AI-Researcher 覆盖文献理解、创新方案、实现、实验和写作，并使用 Scientist-Bench 研究其能力。它们共同说明，角色划分必须落实到具体产物和交接，而不是只给 Agent 安排不同头衔。[Agent Laboratory](https://arxiv.org/abs/2501.04227)；[AI-Researcher](https://arxiv.org/abs/2505.18705)

对 AutoResearch 的意义是把“本阶段完成”定义为交付了什么：方案、运行记录、证据说明、实验报告。最终论文只是可能的产物之一，不能代替研究闭环本身。

### 4.3 AgentRxiv：报告也是跨团队记忆

AgentRxiv 引入共享预印本平台，让自主研究团队提交和读取研究成果，在后续研究中复用已有工作。其实验场景包括数学推理相关的研究改进，不能直接当成大规模 ML 训练调度的验证。[论文](https://arxiv.org/abs/2503.18102)；[项目页](https://agentrxiv.github.io/)

与实验状态共享相比，报告层面的共享更容易跨任务阅读，却可能损失运行细节。本文的推论是：Paper Agent 的阶段报告可以成为记忆入口，但仍需链接到具体 Spec 与实验产物，不能用一段叙述替代全部证据。

### 4.4 R&D-Agent：研究反馈与执行反馈分开处理

R&D-Agent 把数据科学任务组织为 Research 与 Development 两类活动，在迭代中生成方案、实现并利用反馈。v2 标题为 *R&D-Agent: An LLM-Agent Framework Towards Autonomous Data Science*；早期版本的长标题不同。[论文与版本记录](https://arxiv.org/abs/2505.14738)；[开源项目](https://github.com/microsoft/RD-Agent)

对 AutoResearch 的直接启发是区分两类失败：代码运行错误通常由 Implementation 修复；实验已经有效执行、但不能支持提出的结论，则需要研究组重新考虑问题或设计。这两类反馈不应共用一个含糊的“重试”动作。数据科学基准上的得分提升，也不能直接代表开放式科学发现能力。

### 4.5 Virtual Lab 与 Co-Scientist：讨论、批评和假设演化

Virtual Lab 通过 PI、Scientific Critic 和领域专家的会议式协作开展研究，并在纳米抗体设计中结合真实实验验证。其实际科学价值离不开人类提供的目标、指导与实验工作；它不是一套无人值守的物理实验室。[Nature 论文](https://www.nature.com/articles/s41586-025-09442-9)；[开源项目](https://github.com/zou-group/virtual-lab)

Co-Scientist 通过生成、评审、排序与演化等过程反复改进假设，重点包括生物医学研究。首版题为 *Towards an AI co-scientist*，2026 年修订为 *Accelerating scientific discovery with Co-Scientist*。本文将它作为方法参考，没有把它列为已经核实具备完整开源运行系统的项目。[论文与版本记录](https://arxiv.org/abs/2502.18864)

对 AutoResearch 的启发是让 Critic 的意见成为具体研究输入：指出混杂因素、设计可反驳的观察、补充对照。讨论或投票本身不能替代实验，尚待实验回答的分歧也不必强行达成一致。

### 4.6 karpathy/autoresearch：简单实验循环的工程参照

该仓库将可修改范围、固定评价、有限运行预算和结果记录写得很具体：Agent 修改训练代码，运行实验，根据结果保留或舍弃尝试。它适合观察一个紧凑实验循环如何成立。[实验协议](https://github.com/karpathy/autoresearch/blob/master/program.md)

它与当前 AutoResearch 的权限边界不同：前者允许执行研究的 Agent 广泛修改训练方法；本项目要求 Implementation 遵守研究组确定的固定项。值得借鉴的是约定清晰、每次尝试留痕，而不是把其优化目标或修改权限原样搬过来。

## 5. 横向比较：比较机制，不拼接排行榜

下表是对前述论文和仓库的归纳，角色名称并不意味着各系统职责完全等价。

| 维度 | The Agentic Scientist | AutoScientists | EvoScientist |
|---|---|---|---|
| 主要关注 | 完整研究流程与可检查的交付 | 长期实验中的团队协作 | 从研究轨迹中积累可复用经验 |
| 组织方式 | 外层编排编码 Agent 和研究阶段 | 围绕假设组织团队，运行层另有协调 | 论文以 RA / EA / EMA 划分逻辑职责 |
| 实验推进 | 按任务约定进行多次尝试及后续分析 | 提案、队列、执行、反馈，任务配置有差异 | 方案搜索与实验实现搜索 |
| 共享信息重点 | 提炼后的研究记忆与结论 | 讨论、提案、实验状态、结果 | 研究记忆与工程记忆 |
| 适合借鉴 | Spec、评价条件、证据与报告关联 | 资源执行分工、跨线反馈、长期运行 | 经验提炼、按需检索、避免重复踩坑 |
| 需要谨慎迁移 | 阶段降级与论文产出政策 | 动态组队与队列认领协议 | 当前仓库角色不能直接等同论文角色 |

三个系统的任务、基础模型、预算和评价方式不同。不能拿一个系统的想法评分、另一个的基准排名和第三个的论文接收结果排出统一高低。对于本项目，判断借鉴价值更合适的问题是：这个机制解决了什么具体失败模式，为此增加多少计算和协调成本？

### 代表性评估证据

| 工作 | 作者报告的评估或结果 | 对架构判断的意义与限制 |
|---|---|---|
| The Agentic Scientist | 在不同计算研究任务和编码 Agent 组合上评估；开发版本用于共同完成一篇被 ALIFE 2026 接收的论文 | 支持编排与研究流程的可行性；共同研究案例不能当作完全无人参与的成功率。[论文摘要](https://arxiv.org/abs/2610.18933) |
| AutoScientists | BioML-Bench 24 项任务平均排行榜百分位为 74.4%；在其 GPT 训练优化设置中，达到目标验证指标的速度约为 Autoresearch 基线的 1.9 倍 | 显示特定任务和匹配实验预算下的收益；74.4% 是排行榜百分位，不是准确率或实验成功率，1.9 倍也不是通用加速比。[论文摘要](https://arxiv.org/abs/2605.28655) |
| EvoScientist | 与 7 个开源或商业基线比较想法质量，并研究记忆演化对代码执行成功率的影响 | 为经验积累提供实验支持；想法质量仍依赖自动与人工评审，执行成功也不等于科学结论成立。[论文摘要](https://arxiv.org/abs/2603.08127) |

这些是作者报告的结果，本文未独立复测。相比只记住最优数字，阅读消融时更应关注基础模型、可用工具、实验次数、计算预算和人工介入是否保持一致。

## 6. 跨论文综合：对架构真正有影响的几个问题

以下是本文的综合分析，不是某一篇论文已经证明的结论。

### 6.1 共享记忆需要区分三层

| 层次 | 回答的问题 | 在 AutoResearch 中的对应 |
|---|---|---|
| 当前工作状态 | 什么正在做、等待什么、由谁负责？ | 研究线状态、运行申请、提交版本 |
| 原始证据 | 哪次运行、什么配置、产生哪些结果？ | Spec、日志、指标、产物引用 |
| 提炼的研究经验 | 下次遇到类似问题应该注意什么？ | Boss 维护的研究 / 工程记忆树 |

共享一个聊天文件并不能同时解决三类问题。树形记忆适合导航和摘要；底层实验产物保持可追溯，其他角色按索引只读取相关条目。横跨多个研究方向的经验，可以通过引用关联，避免为适应目录层级反复复制内容。

Boss 单独维护有利于控制一致性，但也意味着周期内尚未送达 Boss 的失败经验不会立即成为全局记忆。这是当前设计的时效性取舍：应通过后续实际运行评估是否产生大量重复探索，不必现在就改变写入权限。

### 6.2 “验证”应按对象和成本定义

至少要区分：方案是否能回答问题、执行是否完成、已有结果能否支持结论，以及独立重跑能否得到相近结果。它们不是同一种检查。

本项目已选择：Implementation 负责执行与监控；Verify 默认实现可信，使用已有产物与轻量 CPU 分析，检查方案和结果关系。因而 Verify 通过代表“当前证据足以支持限定范围的结论”，不代表完成独立复现。若需要新的正式实验，那是下一轮研究任务，经 Organizer 安排资源，不应藏在 Verify 内部消耗同等规模的算力。

### 6.3 科学失败、执行失败、证据不足应分别保存

有效负结果可能说明某个设想在指定条件下没有效果；执行失败可能只说明实现或资源存在问题；证据不足可能意味着目前无法判断。把三者都压缩成“失败”，会让记忆产生错误经验，也会让 Boss 错误分配下一轮预算。

同样，提前终止只证明实际观察到的部分。只有在预先约定且适用的停止规则支持下，才能据此作相应范围的判断；不能把“没有跑完”自动写成“假设已被否定”。

### 6.4 逐级验证用于控制成本，不应成为小实验崇拜

轻量验证可以提前排除明显定义错误、统计计算错误或不可执行的方案，却不能保证筛出只有在大规模下才出现的效果。适合本项目的规则是：研究组解释每一级验证要消除什么不确定性；需要规模才能回答的问题可以直接设计正式实验，由 Organizer 安排。无需把“小实验先获得正收益”设成所有方向的门槛。

### 6.5 多 Agent 的价值需要对照，角色数量不是贡献本身

分角色可能减少某些认知盲点，也增加通信成本、重复阅读和责任转移。共享同一模型和同一记忆的角色，还可能产生高度相关的判断。后续应比较相同总预算下的有效产出，不能只观察多 Agent 系统最终找到了一个好结果。

## 7. 与当前 design.md 的对应关系

本节依据 2026-10-24 的 [design.md](./design.md)。以下“已确定”描述当前方案；“后续检验”只是评估建议，不修改已有决策。

```text
研究线 i：Exploration_i ↔ Critic_i → Implementation_i → Verify_i
                                       ↑
                         Organizer 批准并返回同一实现者

Verify：普通轮次通过 → Boss；未通过 → 本线 Exploration / Critic
        第 k 轮，无论通过与否 → Boss，不同时退回研究组

全局：Organizer、Boss Group、Boss 独占写入的树形共享记忆
报告：Paper Agent 编写阶段实验报告
```

上图省略 Implementation 向 Organizer 提交运行申请等细节，只突出角色对应关系与验证分流。

各研究线自 Boss 最近一次下发任务起独立计数，一份完整提交包完成 Verify 算一轮，内部调试和同一提交的多个随机种子不分别计轮。普通轮次通过即可提前交 Boss，第 k 轮则强制交 Boss；Boss 接管并下发新任务后重置计数周期。

| 当前已确定的设计 | 文献启发 | 有意保留的差别 / 后续检验 |
|---|---|---|
| Experiment Spec 贯穿设计、运行、结果与验证，Submission 有版本 | Agentic Scientist 的任务约定与产物组织 | 本项目加入 Exploration / Critic 分歧、周期汇总与 Boss 决策，不能声称论文定义了同一个 Spec |
| 每条研究线固定配套四类角色 | 多系统的职责分解 | 不采用 AutoScientists 的动态组队；以后观察固定配套是否造成闲置或思路固化 |
| Implementation 写代码、运行、监控、自行修复执行错误 | 编码 Agent 的实验闭环、R&D 分工 | 不允许自行改研究方案；调整范围必须在 Spec 中明确 |
| Organizer 批准后交回对应 Implementation 执行 | AutoScientists 的资源协调与执行分离 | 不采用对外任务认领；内部调度方法待实现阶段讨论 |
| Verify 单独设置，只验证方案与已有结果 | 研究评审与证据检查思想 | 不承担大规模重跑、独立复现或常规逐行代码审计 |
| 每 k 轮无论结果如何直达 Boss | 对长期循环失去全局可见性的回应 | 是本项目治理规则，文献没有给出通用最优 k；每线独立计数 |
| Boss 唯一维护树形共享记忆 | EvoScientist 的持久经验、AgentRxiv 的知识复用 | 单写入权限与树结构是本项目选择；评估整理成本和共享延迟 |
| 轻量验证与正式实验区分，保留负结果和停止原因 | 多阶段实验与反馈积累 | 不要求小规模先成功；避免把早停结果包装成完整实验结论 |
| Paper Agent 写当前进展报告 | 各系统将研究过程转化为可读成果 | 最终论文另设流程；阶段报告不以“讲成成功故事”为目标 |

这里没有必要把 Boss 再改成默认的实验复现者。科学审查、项目取舍和共享记忆管理可以分开：Verify 给出证据边界，Boss 决定是否继续投入。第 k 轮汇报只是改变接收方，不会改变“未通过”的验证状态。

## 8. 后续如何验证这些借鉴是否有效

以下为未来评估建议，不涉及现在暂缓讨论的消息系统、锁、持久化或调度算法实现。

| 要检验的问题 | 可比较的条件 | 应记录的结果 |
|---|---|---|
| 共享记忆是否节省研究资源？ | 无共享 / 原始历史 / Boss 提炼索引；其他条件尽量一致 | 重复实验数、错误经验复用、读取与整理成本 |
| Critic 是否改善实验设计？ | 有 / 无 Critic，在总预算匹配下比较 | 运行前纠正的问题、无效实验比例、协调耗时 |
| Verify 是否改善结论质量？ | 使用已有产物验证 / 直接汇报 | 不受支持的结论、遗漏局限、误拦有效负结果 |
| k 如何影响全局判断？ | 若干预设 k，按研究线独立计数 | Boss 介入延迟、无效循环消耗、Boss 工作量 |
| 多条线是否带来真实多样性？ | 单线与多线，总计算及模型预算匹配 | 方法差异、重复方向、有效发现及资源效率 |
| 阶段报告是否帮助下一轮？ | 仅结果表 / 带证据引用的报告 | 新任务理解成本、错误引用、经验遗漏 |

评价时分开记录 GPU/CPU 消耗、模型调用成本、墙钟时间和人工介入。报告全部尝试的分布及失败情况，而不只报告最优结果。新的对照实验属于系统评估计划，不意味着让日常 Verify 复现每一次正式实验。

## 9. 参考文献与阅读顺序

建议先读 [1] 的研究约定与流程，再读 [2] 的团队和执行协作，最后读 [3] 的记忆演化；当前方案最需要借鉴的内容集中在这三篇。其余按问题查阅，无需逐篇照搬架构。

1. **Lange, R. T., et al. The Agentic Scientist: Orchestrating Coding Agents for Automated Scientific (Co-)Discovery.** 2026. arXiv:2610.18933，v1。[论文与日期](https://arxiv.org/abs/2610.18933) · [全文](https://arxiv.org/html/2610.18933v1) · [开源仓库](https://github.com/SakanaAI/AI-Scientist-v3) · [官方介绍](https://pub.sakana.ai/agentic-scientist/)
2. **Gao, S., Fang, A., & Zitnik, M. AutoScientists: Self-Organizing Agent Teams for Long-Running Scientific Experimentation.** 2026. arXiv:2605.28655，v1。[论文与日期](https://arxiv.org/abs/2605.28655) · [全文](https://arxiv.org/html/2605.28655v1) · [开源仓库](https://github.com/mims-harvard/AutoScientists) · [项目页](https://autoscientists.openscientist.ai/)
3. **Lyu, Y., et al. EvoScientist: Towards Multi-Agent Evolving AI Scientists for End-to-End Scientific Discovery.** 2026. arXiv:2603.08127，本文参考 v2。[论文与版本](https://arxiv.org/abs/2603.08127) · [v2 全文](https://arxiv.org/html/2603.08127v2) · [开源仓库](https://github.com/EvoScientist/EvoScientist)
4. **Lu, C., et al. The AI Scientist: Towards Fully Automated Open-Ended Scientific Discovery.** 2024. arXiv:2408.06292。[论文与版本](https://arxiv.org/abs/2408.06292) · [开源仓库](https://github.com/SakanaAI/AI-Scientist)
5. **Yamada, Y., et al. The AI Scientist-v2: Workshop-Level Automated Scientific Discovery via Agentic Tree Search.** 2025. arXiv:2504.08066。[论文与日期](https://arxiv.org/abs/2504.08066) · [开源仓库](https://github.com/SakanaAI/AI-Scientist-v2)
6. **Schmidgall, S., et al. Agent Laboratory: Using LLM Agents as Research Assistants.** 2025. arXiv:2501.04227。[论文与版本](https://arxiv.org/abs/2501.04227) · [开源仓库](https://github.com/SamuelSchmidgall/AgentLaboratory)
7. **Schmidgall, S., & Moor, M. AgentRxiv: Towards Collaborative Autonomous Research.** 2025. arXiv:2503.18102。[论文与日期](https://arxiv.org/abs/2503.18102) · [项目页](https://agentrxiv.github.io/)
8. **Tang, J., Xia, L., Li, Z., & Huang, C. AI-Researcher: Autonomous Scientific Innovation.** 2025. arXiv:2505.18705。[论文与日期](https://arxiv.org/abs/2505.18705) · [开源仓库](https://github.com/HKUDS/AI-Researcher)
9. **Yang, X., et al. R&D-Agent: An LLM-Agent Framework Towards Autonomous Data Science.** 2025. arXiv:2505.14738，v2 标题；v1 题为 *R&D-Agent: Automating Data-Driven AI Solution Building Through LLM-Powered Automated Research, Development, and Evolution*。[论文与版本](https://arxiv.org/abs/2505.14738) · [开源仓库](https://github.com/microsoft/RD-Agent)
10. **Swanson, K., et al. The Virtual Lab of AI agents designs new SARS-CoV-2 nanobodies.** Nature, 2025。2024 年已有预印本，题名与正式版不同。[正式论文](https://www.nature.com/articles/s41586-025-09442-9) · [首版预印本](https://www.biorxiv.org/content/10.1101/2024.11.11.623004v1) · [开源仓库](https://github.com/zou-group/virtual-lab)
11. **Gottweis, J., et al. Accelerating scientific discovery with Co-Scientist.** arXiv:2502.18864，2026 年 v2 标题；2025 年 v1 题为 *Towards an AI co-scientist*。[论文与版本](https://arxiv.org/abs/2502.18864) · [v1 全文](https://arxiv.org/html/2502.18864v1)
12. **Karpathy, A. autoresearch.** 仓库型工程参考，不作为论文条目计数。[仓库](https://github.com/karpathy/autoresearch) · [实验协议](https://github.com/karpathy/autoresearch/blob/master/program.md)

### 查阅实现时优先看的文档

- Agentic Scientist：[流水线](https://github.com/SakanaAI/AI-Scientist-v3/blob/main/docs/pipeline.md)、[运行与并行配置](https://github.com/SakanaAI/AI-Scientist-v3/blob/main/docs/usage.md)。
- AutoScientists：[协调器 runbook](https://github.com/mims-harvard/AutoScientists/blob/main/runbook.md)、[Analyst 职责](https://github.com/mims-harvard/AutoScientists/blob/main/system/templates/ROLE-ANALYST.md)、[GPU Agent 职责](https://github.com/mims-harvard/AutoScientists/blob/main/system/templates/ROLE-GPU.md)。
- EvoScientist：[README 中当前角色与记忆功能](https://github.com/EvoScientist/EvoScientist/blob/main/README.md)，与论文 v2 的方法部分对照阅读。

本文未复现以上系统的性能，也没有审计其全部代码。论文结果按作者报告理解；关于 AutoResearch 的对应与取舍是面向本项目的分析，需由后续实验判断效果。
