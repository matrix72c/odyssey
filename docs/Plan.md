# Odyssey 实施计划

本文维护任务划分、依赖、早期探测、研究计划与验收状态。需求与 AC 定义见[功能设计](Functional_Design.md)，机制见[架构设计](Architecture_Design.md)，后端事实见[复用清单](Reuse_Map.md)。本文是唯一记录进度的文档，设计文档不写现状。

## 1 协作方式

每个任务有一个交付主责，跨域任务的其他领域负责人提供各自模块内的改动；一人可以承担多个任务。“硬依赖”指验收所需的前置交付；契约代码合入 main 后即可用替身并行编码，不必等待前置模块全部完成。`contracts/`、composition、公共配置/fixture 由集成负责人合入；不同任务避免同时修改同一文件。后端 topic 与 integration 遵循父工作区 AGENTS.md；odyssey 自身的 issue、PR、审查与验证见[开发流程](Development_Workflow.md)。

## 2 任务

| 任务 | 主责 | 硬依赖 | 对应功能 |
| --- | --- | --- | --- |
| TASK-00 公共契约、配置与运行基础 | 集成（Infrastructure 配合） | 无 | FR-11、FR-12、FR-14 |
| TASK-01 基础算法与规则 | Algorithms | TASK-00 | FR-02、FR-06、FR-08 |
| TASK-02 独立任务执行与评估 | Execution | TASK-00 | FR-01、FR-04、FR-11 |
| TASK-03 Experience 与训练使用账本 | Experience | TASK-00、TASK-01 | FR-02、FR-04、FR-05、FR-07、FR-11 |
| TASK-04 长任务接续与多沙盒 | Execution | TASK-02、TASK-05 | FR-01、FR-03、FR-11 |
| TASK-05 Serving 稳定入口与真实轨迹 | Serving | TASK-00 | FR-03、FR-07、FR-09、FR-11 |
| TASK-06 XTuner 训练运行时与 GRPO | Training | TASK-00、TASK-01、TASK-03 | FR-06、FR-07、FR-13 |
| TASK-07 PPO 独立 critic 与异步规则 | Training（Algorithms 配合） | TASK-01、TASK-06 | FR-06、FR-08、FR-11、FR-13 |
| TASK-08 静态 Control 与评测导出 | Control | TASK-02、TASK-03、TASK-09 PUB | FR-02、FR-05、FR-08、FR-11、FR-12 |
| TASK-09 权重发布、真实 PD 与请求接续 | Serving（Training 配合） | TASK-05、TASK-06 | FR-03、FR-08、FR-09、FR-11、FR-13 |
| TASK-10 资源重构与共同恢复 | Infrastructure（Training/Serving 配合） | TASK-07、TASK-08、TASK-09 | FR-10、FR-11、FR-13 |
| TASK-11 统一供给与资源策略 | Control | TASK-08、TASK-10 | FR-10、FR-13、FR-14 |
| TASK-12 真实长任务与故障集成 | 集成 | TASK-04、TASK-07、TASK-08、TASK-09、TASK-10、TASK-11 | FR-01、FR-09、FR-11、FR-12、FR-13 |
| TASK-13 目标规模交付 | 集成/运行 | TASK-12 | FR-12、FR-13、FR-14 |

验收波次：`00 → (01,02,05) → (03,04) → 06 → (07,09) → 08 → 10 → 11 → 12 → 13`，括号内可并行，编码可更早开始。TASK-08 只依赖 TASK-09 的 PUB 检查组（可在非 PD 单引擎组上完成）；PD 检查组依赖 LMDeploy topic，与 TASK-08 并行，不阻塞静态 GRPO 闭环。PPO 控制闭环由 TASK-12 联合 TASK-07 关闭；完整 execution 的跨版本接续由 TASK-12 联合 TASK-04 验收。

### TASK-00 公共契约、配置与运行基础

- 范围：`contracts/`、`infrastructure/` 的 ArtifactStore（petrel 与 gpfs 后端）、PostgreSQL 适配与固定资源执行、配置 loader 与 run 快照、composition、测试 fixture。
- 交付：架构第 3 节的数据类、错误、命令/事件样例与契约 fixture；严格配置校验；PostgreSQL 上的领域记录、run_epoch 与操作去重；Ray detached actor/placement group 的固定组 reserve/bind/launch/terminate/release 与独立资源核对；rank 0 supervisor 与 entrypoint 的 checkpoint 自动恢复；容量表 v0（架构 §8.6）与后端缺口清单。
- 验收：AC-21、AC-27、AC-29 的基础部分。真实 PostgreSQL 上覆盖提交前后中断、回执丢失、重启恢复、重复 ID 内容冲突；事件乱序、未知字段、导入副作用均可自动检查。

### TASK-01 基础算法与规则

- 范围：`algorithms/`、recipe 配置、纯元数据约束。
- 交付：`grpo_v1`、`ppo_v1` 的规则，`proximal_clip_tis` 目标经 XTuner loss 注册接入；采样、过滤、使用次数与版本规则（reserve 按 `expected_actor_version` 检查年龄、wait 只保护事务外 group）；沿 ActionTimeline 的 GAE，含跨计算序列的相邻动作、截断边界上下文 bootstrap 与 credit_scope 边界；独立参考实现不调用被测函数。
- 验收：AC-11、AC-13 的参考数学部分：组标准差、有效零分、mask、终止/截断、token 级分母、固定快照与多 epoch 都有小样本预期值。

### TASK-02 独立任务执行与评估

- 范围：`execution/` 与提取的任务链路、harness 适配。
- 交付：独立 owner、launch/inspect/assess/cancel、原生任务来源、有限并发、评估有效性与清理分离、TaskCatalog、ContextRecord、持久 read_changes、checkpoint/restore 端口。
- 验收：AC-01、AC-06、AC-20 的执行部分；假 ModelClient 与真实服务均可驱动任务；重复 launch 不重复建环境。

### TASK-03 Experience 与训练使用账本

- 范围：`experience/`。
- 交付：ingest 与源游标、ActionTimeline、请求树与 TrainingSequence（`exact`/`approximate` 两种合并模式）、专家记录位置映射、group 完整性、候选查询、绑定 `expected_actor_version` 的原子 reserve、manifest、使用账本、合并率与切分指标、checkpoint/restore（含实际消费游标）。
- 验收：AC-03、AC-06、AC-08、AC-09、AC-10 的数据部分；A/B/C/E 组批与 D/F 延后、重复 chunk/assessment、来源配额、PPO epoch、预留冲突和重启均覆盖。

### TASK-04 长任务接续与多沙盒

- 范围：Execution continuation、harness request gate、子 Agent/多沙盒生命周期；以 sandbox_rl 的 RequestGate 与同 owner 接续为起点。
- 交付：pause/resume/export、工具安全点、累计预算、父子关系/fork/join、恢复能力分级、owner 丢失处置与资源对账。
- 验收：AC-02、AC-04、AC-19 的执行部分；至少主线 harness 有真实接续证据。

### TASK-05 Serving 稳定入口与真实轨迹

- 范围：`serving/`、`backends/lmdeploy/`、ModelClient。
- 交付：CPU gateway 与分区单写者、前缀树复用与请求树记录、scope_seq 分配、对最终引擎采样配置的白名单校验与记录、请求提交（架构 §6.3）、attempt/offset 校验、精确 token/logprob/版本、专家记录的切分判定与 ArtifactStore 写入、流式背压、取消、固定布局实例管理、capability 报告。
- 验收：AC-05 的同版本部分、AC-10 的 Serving 部分；重复 chunk 不重复提交，gateway 崩溃后无重复/缺失，引擎结束不删除已提交输出，训练采样参数越界被拒。

### TASK-06 XTuner 训练运行时与 GRPO

- 范围：`training/`、`backends/xtuner/`，必要的 XTuner topic。
- 交付：configure/prepare/execute/inspect/snapshot/export_hf；GRPO 目标映射与 packing；rank 更新账本；MoE router replay 接入；概率诊断与配对探针。
- 验收：AC-10、AC-11、AC-15；不等长/空贡献 rank、DP/SP、MTP 分母、SKIPPED 与重复 execute 有明确结果；至少两次真实 actor update。
- 性能：actor 单独运行基线、ExecutionProfile/CostProfile（AC-26）。

### TASK-07 PPO 独立 critic 与异步规则

- 范围：Algorithms/Training 的 PPO 计划、目标、快照与 critic 状态。
- 交付：`ppo_v1` 的 sync 与 bounded、固定近端/value 目标、多 epoch、独立版本/optimizer/scheduler、actor/critic 完整 checkpoint（含 RNG）与 async_save_dcp 接入。
- 验收：AC-12、AC-13、AC-15、AC-18 的模型状态部分；新进程恢复后继续更新并核对目标、参数、optimizer 与 scheduler。
- 性能：critic 准备与更新阶段基线、同驻 HBM 的内存峰值（AC-26）。

### TASK-08 静态 Control 与评测导出

- 范围：`control/`、composition、status/inspect/stop、`odyssey eval`。
- 交付：静态计划、准入规则与失败预算、执行并发、组批、训练回执与使用决定、发布（含提前发布条件）、drop/wait 与防死锁、受控停止、sampler、EXPORT 与独立评测命令、训练空闲区间与原因拆分、控制元数据 checkpoint/restore。
- 验收：AC-03、AC-07、AC-08、AC-09、AC-14、AC-15 的控制部分与 AC-25。

### TASK-09 权重发布、真实 PD 与请求接续

- 范围：Serving 发布、P/D handoff、请求迁移/重算、模型加载；LMDeploy topic（架构 §8.2）。
- PUB 检查组：两阶段安装/激活、逐 server 核对与 publication_id、前缀缓存清空、跨版本 cursor 接续、checkpoint-engine 发布路径；可在非 PD 单引擎组完成。
- PD 检查组：P/D 独立实例、按 publication 配对路由、P 侧前缀缓存、完整推理状态交接、首 token logprob 与 P 侧专家记录。
- 验收：AC-05、AC-16、AC-22 的 PD 部分；使用独立驱动器验收，不依赖 TASK-08/04。
- 性能：P/D 基线、发布与状态传输 CostProfile（AC-26）。

### TASK-10 资源重构与共同恢复

- 范围：Infrastructure 资源/进程适配、Training/Serving 重构、Coordinator checkpoint 与恢复。
- 交付：至少两种非零 T 规模、P/D 独立增减、T/P/D 角色转换、状态峰值核算、共同 checkpoint（对象存储写入）与 R0/R1/R2 恢复、R2 的 group 整组退役与补入。
- 验收：AC-17、AC-18、AC-19、AC-21、AC-22。

### TASK-11 统一供给与资源策略

- 范围：Scheduler 策略、观测聚合、搬运并发约束。
- 交付：`fixed` 与 `joint`、有限候选比较、P/D 分别建模、转换成本、最短驻留、固定回退与决策日志、资源转换等空闲主因。
- 验收：AC-24、AC-26；数据不足不盲扩 T，P/D/工具瓶颈可区分，未知观测保守处理。

### TASK-12 真实长任务与故障集成

- 范围：端到端测试、故障注入、验收脚本、必要集成修复和运行手册。
- 交付：AC-01 至 AC-27、AC-29、AC-30 的功能范围证据；AC-23 的两小时联合验收；第 6 节的最小集成场景。
- 验收：每个 AC 有命令、配置、代码 git rev、日志、断言和结果；FAIL/BLOCKED/NOT_RUN 明确保留。

### TASK-13 目标规模交付

- 输入：GLM-5.3 权重、同规模 critic、约千卡获准资源、生产任务与预登记的健康及 GRPO 提升方案。
- 交付：生产 recipe/layout/容量配置、实际资源预测与单作业提交、长程训练、发布、恢复与清理、目标布局下的性能复核。
- 验收：AC-28。

## 3 里程碑

**M1a 静态 GRPO 闭环。** 在固定非 PD 布局上用 qwen3_5_moe 连续完成至少 3 个“组批 → 更新 → 发布 → 新版本生成”循环，账本、版本和清理报告闭合。所需最小子集：TASK-00 契约与固定资源适配；TASK-02 一个可核验任务集的真实 harness 与 verifier；TASK-05 单引擎精确 token/logprob/版本/专家记录与持久提交；TASK-03 组批、预留与使用账本；TASK-06 GRPO prepare/execute、router replay、rank 账本与快照；TASK-09 PUB；TASK-08 静态准入、组批、发布与停止清理。

**M1 静态 GRPO + PD。** M1a 的 Serving 换成经验证的真实 PD 组合，依赖 TASK-09 PD 检查组与 LMDeploy topic。

**研究门槛。** M1a 之后执行第 5 节的门槛测量，决定 TASK-10/11 的投入。

## 4 早期探测

以下能力决定后续任务能否成立，在对应任务的第一阶段实测并写入能力报告。

| 编号 | 探测 | 任务 | 结果 |
| --- | --- | --- | --- |
| P-01 | 现有 sandbox_rl 栈上各 harness（Claude Code 压缩、多 user 轮、GLM 模板）的 SessionServer 前缀命中率 | TASK-05 | 未测 |
| P-02 | LMDeploy PD 角色下开启 P 侧前缀缓存的改动量与正确性 | TASK-09 | 未测 |
| P-03 | 混合线性注意力模型的 PD 状态迁移 | TASK-09 | 未测 |
| P-04 | 权重更新后前缀缓存清空，新版本请求不复用旧缓存 | TASK-05 | 未测 |
| P-05 | 专家记录从 prefill/decode 经请求树到 XTuner actor 回放逐位置一致；同版本内前缀缓存命中与未命中时返回的前缀专家记录是否一致 | TASK-05/03/06 | 未测 |
| P-14 | `exact` 与 `approximate` 合并模式在同参数、同输入下的 logprob、loss、梯度差异，以及合并率与训练开销；之后在固定布局上对照学习效果，决定默认模式 | TASK-03/06 | 未测 |
| P-06 | gateway 接收完整 prompt 专家记录的传输成本（约 0.6 KB/token） | TASK-05 | 未测 |
| P-07 | XTuner 冻结目标后只训练的入口 | TASK-06 | 未测 |
| P-08 | XTuner 在不同 DP/EP 规模之间重载模型与 optimizer 分片 | TASK-06 | 未测 |
| P-09 | RNG 状态保存与恢复 | TASK-07 | 未测 |
| P-10 | 缩减层数 GLM-5.3 的 RL 训练、无 MTP 的 critic、checkpoint-engine 权重更新 | TASK-06/07/09 | 未测 |
| P-11 | RJob：一个 pod 失败后作业立即失败还是等待 `grace-period-minutes`；自愈重启整个作业还是只替换故障 pod | TASK-00 | 未测 |
| P-12 | rjob pod 到集群 PostgreSQL 的连通性与提交延迟 | TASK-00 | 未测 |
| P-13 | 对象存储与 GPFS 的聚合写带宽；GLM 规模 checkpoint 写入耗时；`--share-host-shm` 下的暂存容量 | TASK-07/10 | 未测 |

## 5 研究计划

研究主线：在有界异步约束下联合调度训练数据供给与 T/P/D 资源。最接近的已有工作 QwenGyre（rollout 与训练之间的弹性 GPU 切换）已覆盖 T↔rollout 切换，论文贡献须落在它未覆盖的部分。以下均不是框架交付要求。

**研究门槛（M1a 之后）。**

1. 空闲上界：固定布局下跑真实长任务负载，记录 T、P、D 各自的空闲比例及其随时间的波动（复用训练空闲归因）。
2. 切换成本：缩减层数 GLM 上实测一次 T 组重分片与节点改作推理的完整耗时。

若“可回收空闲 − 摊销切换成本”不显著，TASK-10/11 降级为最小实现，研究重心转向下表的其他候选。

| 候选机制 | 来源 | 切换代价 | 说明 |
| --- | --- | --- | --- |
| T↔P/D 联合调度 | 原主线 | 分钟级 | 受门槛约束 |
| P↔D 动态配比 | 发布后重新 prefill 的负载突增 | 秒级（权重从 host 或参数服务加载） | 长程 Agent RL 特有，优先评估 |
| D 侧会话保留与增量 KV | 多轮 PD 每轮传完整上下文 | — | 需要 LMDeploy 侧改动 |
| 主机内存参数服务 | 发布离开训练关键路径 | — | 以 XTuner checkpoint-engine 的 `ParameterServer` 为起点，同时服务 checkpoint 暂存、导出与新副本加载 |

**对照。** 内部基线为扫描配比后最优的 `fixed`（AC-24）。外部基线作为论文任务，在中等规模（qwen3_5_moe、64–128 卡）上用同一 harness 与任务集，通过 OpenAI 兼容入口接入 slime fully-async（优先）与 verl async，比较有效训练进度、GPU 时和空闲比例；QwenGyre 未开源时做定性对比或复现其关键机制。论文数据用同一套事件和开关收集，同时给出质量、有效进度、总资源成本、转换/恢复开销和实际消费分布，不预设加速比例。

## 6 验证

### 6.1 测试分层

| 层级 | 重点 | 运行要求 |
| --- | --- | --- |
| CPU/契约 | 类型、配置、准入、状态机、数据使用、幂等、故障与导入边界 | 不要求 torch/Ray/GPU；逻辑时钟与有限替身 |
| 数值/后端 | GRPO/PPO 参考目标、mask、packing、梯度、optimizer、MTP、分布式归一化 | 已 pin 的训练环境；按 dtype 固定容差并记录原因 |
| 小规模真实闭环 | 多步更新、原始 token、发布、静态 PD、真实 Agent、完整恢复 | 真实 GPU 与沙盒；不用 mock 代替 |
| 多节点长程/故障 | 两小时以上任务、父子多沙盒、弹性重构、失败回退、资源清理 | 实际后端、网络与进程故障 |
| 目标规模 | GLM-5.3 actor + 同规模 critic、约千卡；健康、GRPO 提升、恢复 | AC-28；资源不足如实标记 |

测试目录为 `tests/contracts/`、`tests/numerics/`、`tests/backend/`、`tests/e2e/`，CPU 入口与后端入口随 TASK-00 建立。可提取参考项目的必要用例，但不在 sandbox_rl 下执行或写入测试产物。数值参考使用独立实现和固定小样本，验证目标、梯度方向、分母和一次参数更新；不要求不同并行布局逐 bit 相同，失败时不随意提高容差。

### 6.2 最小集成场景

功能验收模型为 qwen3_5_moe（本地 `Qwen3-6-35B-A3B`）与一个可核验任务集。B=4 的受控场景包含正常组、慢组、零奖励组、基础设施失败组和跨版本接续组。顺序：非 PD 固定布局上的静态 GRPO（M1a）→ 真实 PD → PPO 独立 critic → 一次安全边界转换、故障恢复与停止清理。

AC-23 固定为 GRPO + 有界异步 + PD：同一真实任务持续两小时以上，在仍活跃时依次经历跨版本发布、一次已验证的资源转换、一次 R1（优先推理实例丢失并按 committed cursor 接续），同次运行另注入一次 R0。R2 的整组退役按 AC-18 单独验证，退役后补入不算该长任务接续成功。

AC-25 让主训练在评测期间至少完成一次真实 actor 更新和发布，核对评测读取的导出版本不变、来源与评测 run 身份齐全。

### 6.3 预登记

AC-28 与 AC-30 开跑前在本文登记：任务划分、种子、采样参数、更新数、资源预算、重复次数、健康阈值、提升阈值、置信水平与估计方法；AC-30 走参考路线时另登记参考实现身份、其提升证据与一致容差。登记后不得看过结果再调低门槛。

| AC | 登记项 | 内容 |
| --- | --- | --- |
| AC-28 | — | 未登记 |
| AC-30 | — | 未登记 |

## 7 AC 状态

状态取 PASS、FAIL、BLOCKED、NOT_RUN。证据位置填运行产物路径或测试命令。

| AC | 主要任务 | 状态 | 证据 |
| --- | --- | --- | --- |
| AC-01 | TASK-02 | NOT_RUN | |
| AC-02 | TASK-04 | NOT_RUN | |
| AC-03 | TASK-03、TASK-08 | NOT_RUN | |
| AC-04 | TASK-04 | NOT_RUN | |
| AC-05 | TASK-05、TASK-09 | NOT_RUN | |
| AC-06 | TASK-02、TASK-03 | NOT_RUN | |
| AC-07 | TASK-08 | NOT_RUN | |
| AC-08 | TASK-03、TASK-08 | NOT_RUN | |
| AC-09 | TASK-03、TASK-08 | NOT_RUN | |
| AC-10 | TASK-03、TASK-05、TASK-06 | NOT_RUN | |
| AC-11 | TASK-01、TASK-06 | NOT_RUN | |
| AC-12 | TASK-07 | NOT_RUN | |
| AC-13 | TASK-01、TASK-07 | NOT_RUN | |
| AC-14 | TASK-08 | NOT_RUN | |
| AC-15 | TASK-06、TASK-07、TASK-08 | NOT_RUN | |
| AC-16 | TASK-09 | NOT_RUN | |
| AC-17 | TASK-10 | NOT_RUN | |
| AC-18 | TASK-07、TASK-10 | NOT_RUN | |
| AC-19 | TASK-04、TASK-10 | NOT_RUN | |
| AC-20 | TASK-02、TASK-12 | NOT_RUN | |
| AC-21 | TASK-00、TASK-10 | NOT_RUN | |
| AC-22 | TASK-09、TASK-10 | NOT_RUN | |
| AC-23 | TASK-12 | NOT_RUN | |
| AC-24 | TASK-11 | NOT_RUN | |
| AC-25 | TASK-08 | NOT_RUN | |
| AC-26 | TASK-06、TASK-07、TASK-09、TASK-11、TASK-12 | NOT_RUN | |
| AC-27 | TASK-00 | NOT_RUN | |
| AC-28 | TASK-13 | NOT_RUN | |
| AC-29 | TASK-00、TASK-12 | NOT_RUN | |
| AC-30 | TASK-06、TASK-07、TASK-12 | NOT_RUN | |

## 8 文档同步

实际目录与接口落地后同步到 Code_Map，命令、限制与运行证据同步到 Training_Guide；README 指向设计文档与运行手册。设计变更直接修改对应设计文档；进度、探测结果和证据只写在本文。
