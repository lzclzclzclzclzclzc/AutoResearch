# Disaster-Tweets11：line-01 与 line-03 问题事实记录

记录日期：2026-11-05。下文时间均为北京时间（Asia/Shanghai，UTC+8）。

## 记录范围与来源

- 项目名称：`Disaster-Tweets11`。
- 项目 ID：`research-3025f735dbb24c069315c8541b664123`，下文简称 `R`。
- 核查代码版本：main 提交 `1cb9e55a4ce309381adeb29b8b0a7e53f8911901`。
- 数据来源：本机 `data/business.sqlite` 中的项目、周期、讨论交接、实现、执行、运行、提交与验证记录，以及对应的源文件和实验产物。数据库以只读方式核查。
- 用户于 2026-11-05 08:19 手动终止项目。项目记录的 `status=terminated`、`stop_reason=user_requested`，`terminated_at` 为 08:19:19。
- 本文仅记录两个问题的事实、相关程序行为与证据边界，不包含解决方案。

## 问题一：line-01 已有历史产物，但后续讨论持续将其列为缺失输入

### 1. 历史实验已生成并保存 folds.csv

line-01 第一周期完成过正式实验，并于 2026-11-05 00:00:42 获得一次 `passed=true` 的 Verify。该验证记录 ID 为 `verification-37a7e46423c246b88dece45cdcfc382a`。

成功运行 ID：

```text
R:supervisor:operation:0:line-01:cycle:line:revision-1:spec:implementation-3:run-1
```

该运行对应的 `folds.csv` 登记信息：

| 字段 | 已核实的值 |
| --- | --- |
| artifact_id | `artifact-2eb774640ed6597b074207972ac5e03f` |
| kind | `experiment_artifact` |
| line_id | `line-01` |
| 文件大小 | 905619 字节 |
| SHA-256 | `c75f9b8327637e44b5a7a20a6a192caf75061e5798b506b0b94120536904dd9d` |
| 列 | `id,split,group_id,group_hash,fold` |
| 数据行数 | 10876，其中 train 7613 行、test 3263 行 |
| train 五折大小（fold 0—4） | 1512、1528、1516、1559、1498 |

原始运行输出位置（相对于仓库根目录）：

```text
data/projects/R/workspaces/line-01/epoch-1/revision-1/code-3-runs/attempt-1/outputs/folds.csv
```

产物库保存位置（相对于仓库根目录）：

```text
data/artifacts/c7/5f9b8327637e44b5a7a20a6a192caf75061e5798b506b0b94120536904dd9d/content.csv
```

核查时产物库文件存在，按文件内容重新计算的 SHA-256 与登记值一致。旧周期的 Submission 和 Verify 正文均包含上述 `artifact_id`。

因此，`folds.csv` 是 line-01 历史实验生成、由系统产物库保存的本线文件，并非只由 Boss 持有的文件。

### 2. 后续讨论中的缺失声明与 Boss 请求

2026-11-05 02:55:07，Boss 为 line-01 建立了方向代次为 3 的新周期。该周期 ID 为：

```text
R:supervisor:operation:2:line-03:cycle:line:revision-1:spec:submission:boss:operation:0:line-01:cycle
```

ID 中出现 `line-03` 是因为这次 Boss 任务来自 line-03 的提交；该周期记录的实际 `line_id` 为 `line-01`。

在这个新周期的交接产物中，成员将 `outputs/folds_line01.csv` 和 `outputs/submission_final.csv` 列为 `BLOCKED_UNVERIFIED`，并写有以下内容：

> 本线可读输入目录不含已核验冻结 folds.csv；禁止用再生成折位替代。

`handoff-1` 的 Critic 交接产物还包含 `requests_to_boss` 字段，其中一项为：

> 转交已核验冻结 folds.csv 的 artifact_id（或指定持有方执行 scripts/export_alignment.py）

同一字段还请求转交真实 `S4_union_kw test_probs` 的产物编号。后续交接仍保留相关缺失声明，部分正文使用了“只有 Boss 能提供”的表述。

这些是成员在讨论产物中的说法；其中“只有 Boss 能提供”不等同于实际权限或文件归属。用户在本次排查中明确说明，其设定不允许 Explore/Critic 向 Boss 索要文件。

### 3. 输入目录与产物库不是同一个目录

成员的缺失说明引用的是对“本线可读输入目录”的检查。用户选择的原始数据目录与系统保存正式实验输出的 `data/projects/...`、产物库 `data/artifacts/...` 属于不同位置。

在输入目录中未找到 `folds.csv`，不等于系统没有保存这个历史实验产物。上面的文件检查确认了历史产物仍然存在。

证据读取工具 `read_research_evidence` 要求调用者提供 `artifact_id`。`backend/adapters/gateway.py:231` 起的检查以项目和研究线归属为依据：Explore、Critic、Implementation、Verify 不能读取其他研究线的原始证据，但没有按新旧周期禁止读取本线历史产物。

### 4. 新周期上下文与实际流转

`backend/graphs/engine.py:960` 构造设计上下文时，`previous_verifications` 的查询条件为：

```python
self.store.list("verification", research_id, cycle_id=state["cycle_id"])
```

该字段只查询当前周期，不会自动把旧周期的 Verify 放入其中。这个设计上下文也没有自动附带本线全部历史产物索引。旧周期的 Submission、Verify 与产物记录仍保存在数据库中。

方向代次 3 的四份 Critic 交接记录如下。每份记录的 `status` 均为 `delivered`、`decision_source` 均为 `unanimous`：

| 交接记录尾部 | next_step |
| --- | --- |
| `:revision-1:critic:group-v3` | `exploration` |
| `:revision-1:handoff-1:critic:group-v3` | `exploration` |
| `:revision-1:handoff-2:critic:group-v3` | `exploration` |
| `:revision-1:handoff-3:critic:group-v3` | `exploration` |

`backend/graphs/engine.py:886` 起的 `design_discussion_v2` 在 Critic 的 `next_step=exploration` 时，再次调用 Exploration，然后继续进入 Critic。该分支没有依据 `requests_to_boss` 或 `blocked` 字段派发 Boss 任务。

核查项目全部 documents 记录时，没有发现 kind 名称包含 `depend` 的记录。这里的请求保存于讨论交接产物中，没有对应的依赖协调记录；这项观察本身不表示用户要求建立此类协调任务。

截至用户终止时，这个新周期没有完成新的 Implementation 与 Verify。line-01 生命周期内已有一次 Verify 通过；后续周期的 `completed_verify_rounds=0` 不表示第一周期没有完成验证。

## 问题二：line-03 新旧周期复用目录，且新实现交付的是占位内容

### 1. 已完成的第一周期与后续周期

line-03 第一周期 ID：

```text
R:supervisor:operation:2:line-03:cycle
```

2026-11-05 的相关记录：

| 时间 | 记录事实 |
| --- | --- |
| 02:14:44—02:14:46 | `implementation-1:run-1` 执行失败，退出码为 2 |
| 02:29:48—02:42:23 | `implementation-2:run-1` 完成，退出码为 0 |
| 02:50:58 | Verify 提交 `passed=true`，记录 ID 为 `verification-1b10aab676734cebb03a20eddd880b1f` |
| 02:55:07 | Boss 继续 line-03，建立新周期，方向代次仍为 1 |
| 05:05:18—05:30:48 | 新周期 Implementation 模型执行完成 |

新周期 ID：

```text
R:supervisor:operation:2:line-03:cycle:line:revision-1:spec:submission:boss:operation:1:line-03:cycle
```

新周期的研究线任务最终记录为 `failed`，错误为：

```text
code: attempt_source_changed
message: 实验尝试源文件与原实现不一致
```

### 2. 两个不同实现记录使用同一个 cwd

第一周期的 `implementation-1` 与新周期的 `implementation-1` 是不同的实现记录，绑定不同的 spec，但保存的 `cwd` 相同：

```text
data/projects/R/workspaces/line-03/epoch-1/revision-1/code-1
```

| 实现记录 | code_version |
| --- | --- |
| 第一周期 `implementation-1` | `f1aac002c446d10aad39971a63c14e44448f2f863423c57b5daaba93427edfab` |
| 新周期 `implementation-1` | `8f8108f13bafc2ccd5f6f222eafc12f877bfe7f3a728dc9a85d8eec3ce07378e` |

`backend/graphs/engine.py:1086` 构造实现目录时使用研究线、`direction_epoch`、`revision` 与 `code_attempt`，没有使用周期 ID。两个周期在这些取值上相同，均落到上述目录。

代码随后逐个以 `write_text` 写入新实现文件。核查时，这个目录中的新交付文件均与新实现记录一致；其中同名的旧源文件已被新内容覆盖。

正式运行的尝试目录为：

```text
data/projects/R/workspaces/line-03/epoch-1/revision-1/code-1-runs/attempt-1
```

该目录在第一周期的第一次运行时已存在。新周期准备第一次运行时再次定位到这个目录。文件核查结果为：

- 两份实现有 8 个同名文件。
- 尝试目录中这 8 个文件的内容均与第一周期实现记录一致。
- 这 8 个文件的内容均与新周期实现记录不同。

`backend/graphs/engine.py:1291` 在准备尝试目录时，检查已有文件字节是否与本次实现一致；发现差异后抛出 `attempt_source_changed`。这个错误发生在该次正式实验启动之前。数据库中没有该新周期的正式 run 记录。

### 3. 新周期实际交付的 10 个文件均为占位内容

新周期 Implementation 对应的模型执行 ID：

```text
17b3a9a45564cb775a9a453ad111dbdf22bc9107f4e3d60af9013206bf750cb8
```

该 execution 的状态为 `completed`。其结构化 `output.files` 与保存的 implementation 文件内容一致，并非界面显示截断：

| 文件 | 完整内容 | UTF-8 字节数 |
| --- | --- | --- |
| `build_line01_contract.py` | `PLACEHOLDER_CONTRACT` | 20 |
| `group_key_audit.py` | `PLACEHOLDER_AUDIT` | 17 |
| `interpreter_check.py` | `PLACEHOLDER_INTERP` | 18 |
| `prepare_vectors.py` | `PLACEHOLDER_VECALL` | 18 |
| `run_fallback.py` | `PLACEHOLDER_RUNFB` | 17 |
| `run_fold.py` | `PLACEHOLDER_RUNFOLD` | 19 |
| `run_reference.py` | `PLACEHOLDER_RUNREF` | 18 |
| `s1_utils.py` | `PLACEHOLDER_S1_UTILS` | 20 |
| `train_s1.py` | `PLACEHOLDER_TRAIN` | 17 |
| `train_vectors_one.py` | `PLACEHOLDER_VEC1` | 16 |

第一周期 `implementation-1` 的 `train_s1.py` 为 30588 字节；新周期同名入口文件为 17 字节。

新实现同时返回了下列参数列表，列表项全为选项名，没有相应路径值：

```json
["--data-dir", "--out-dir", "--line01-folds", "--line01-test-ids", "--group-key-candidate"]
```

该次运行在源文件一致性检查处已被阻断，因此没有实际执行这些占位文件，也没有该次命令的运行结果。

### 4. 实现接收时的检查事实

`packages/contracts/research.py:314` 中，`Implementation.files` 的类型为 `dict[str, str]`。结构合同本身没有对上述占位内容作语义判定。

`backend/graphs/engine.py` 的实现接收流程检查冻结配置、写入路径边界及入口文件存在性，然后保存实现记录。本次占位内容被接收、写入 `code-1` 并登记了新的 `code_version`。

目录复用与占位交付是两个同时存在的事实：前者触发了这次 `attempt_source_changed`，后者说明新实现记录中的文件不包含正文所描述的完整程序。`PLACEHOLDER_TRAIN` 这类单独标识符可以构成合法 Python 语法；文件存在或语法可解析，不代表具有对应的实验实现。

截至本次核查，旧实现的数据库内容和旧尝试目录中的对应文件仍保留；共享 `code-1` 目录中的同名文件已被新实现内容覆盖。line-03 生命周期内完成过一次 Verify，新周期没有完成新的正式实验和 Verify。
