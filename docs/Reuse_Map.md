# Odyssey 复用与适配清单

本清单是后端事实的唯一来源：各模块从哪些现成实现开始、当前 pin 下的实际行为，以及 Odyssey 需要补充的部分。机制设计见[架构设计](Architecture_Design.md)。表中事实来自读码，运行验证的结果记在[实施计划](Plan.md)。

## 使用方式

优先通过依赖模块调用现有计算与执行机制，适配层只转换公共记录、补充领域身份并协调生命周期。内部入口不便调用时，先在后端增加薄接口；确认能力或语义不足时才实现差异。生产实现复用与独立参考计算分开，数值对照不能调用同一被测函数。

XTuner/LMDeploy/Ray 类型留在后端或基础设施适配层。sandbox_rl 模块按需提取到 Odyssey，在提取处注明来源与许可证并重新验收，不在运行时导入。

## Pin

| 仓库 | 当前 pin | 说明 |
| --- | --- | --- |
| XTuner | `int-20261026-1536`（`2d40222a`） | integration 含上游 WIP PPO PR #2132；Odyssey 用到的 `critic_worker`、`critic_loss`、`advantage/gae.py`、`trainer/schedule.py` 只存在于该 PR，合入上游前随 integration 携带 |
| LMDeploy | `b95a5010` | 内容已在上游；Odyssey 的 PD topic 出现后接入 integration 重建链路 |
| Lagent | `46e07b9`（upstream v1.0.0） | 无本地 topic |

三个仓库与 sandbox_rl 共用同一套 `-run` worktree，分支与 pin 规则见父工作区 AGENTS.md。下文路径相对各仓库根目录。

## XTuner

### 可复用入口

| 能力与入口 | 复用方式 | Odyssey 补充 |
| --- | --- | --- |
| GRPO advantage：`xtuner/v1/rl/advantage/grpo.py` | 直接复用组内均值与 `rewards.std()`（样本标准差） | 无 |
| policy loss 与 token 级分母：`xtuner/v1/rl/loss/loss_fn.py` 的 `register_policy_loss`、`xtuner/v1/rl/loss/grpo_loss.py` 的 `GRPOLossContext.build_batches` | 复用注册入口与跨 DP all_reduce 的全局有效 token 分母 | 注册 `proximal_clip_tis` 目标（见下方差异表） |
| 概率、KL、entropy 与分块 loss：`xtuner/v1/loss/base_loss_ctx.py`、`xtuner/v1/rl/loss/base_loss.py`、`xtuner/v1/loss/mtp_loss.py`、`xtuner/v1/rl/rollout_is.py` | 复用分块前向、概率抽取、SP 数据处理与逐 token 计算 | 映射 behavior/prox/current 概率与校正参数；算法开关不从后端默认值隐式继承 |
| packing 与数据准备：`xtuner/v1/rl/trainer/pack.py` 的 `RLDataPacker`、`xtuner/v1/data_proto/sequence_context.py`、`xtuner/v1/rl/trainer/worker.py` | 复用索引级 packing、DP 分派、padding 与 SP 切分 | 接入冻结 manifest 与请求树来源映射；packer 只重排已选数据 |
| MoE router replay：`xtuner/v1/module/router/greedy.py`、`noaux_router.py`、`xtuner/v1/module/decoder_layer/moe_decoder_layer.py`、上述数据准备链路、`xtuner/v1/rl/rollout/utils.py` 的历史专家拼接 | 直接复用专家选择回放、当前参数下的专家权重、索引 dtype 与 offload 处理 | 接入持久专家记录与版本；不另写回放内核 |
| PPO critic：`xtuner/v1/rl/trainer/critic_worker.py` 的 `collect_values/forward_only/fit/offload/onload`、`xtuner/v1/rl/loss/critic_loss.py` | 复用 value head、value 前向、clipped value loss、独立 optimizer/scheduler | 把冻结的 `old_values/value_return` 接到更新入口，按 model_id 记录阶段与回执 |
| GAE：`xtuner/v1/rl/advantage/gae.py` 的 `terminal_rewards/action_gae/compute_ppo_targets/normalize_actor_advantages` | 完整终止序列的数值部分复用 | 截断 bootstrap 与 credit_scope 边界（见差异表） |
| 权重传输：`xtuner/v1/rl/weight_update/weight_iterator.py`、`transport.py`、`update_weighter.py` | 复用 HF 键映射、EP 专家分片、dtype 分桶与 IPC/NCCL/checkpoint-engine 三种传输 | 附加 publication_id、逐接收组核对与两阶段激活 |
| checkpoint 与导出：`xtuner/v1/engine/train_engine.py` 的 `save_dcp/load_dcp/async_save_dcp/save_hf/async_save_hf`、`xtuner/v1/rl/trainer/schedule.py`、`xtuner/v1/utils/async_save_monitor.py` | 复用 DCP、shm 暂存的异步保存、HF 导出、scheduler 保存恢复 | RNG 保存、对象存储 StorageWriter、领域切点与共同 manifest |
| 前缀复用与协议转换：`xtuner/v1/rl/rollout/session_server.py`、`chat_template.py`、`lmdeploy.py`、`trace_store.py` | 复用前缀树匹配、消息规范化、OpenAI/Anthropic 转换、流解析与原始 token/logprob 获取 | 持久请求记录、cursor 与提交后可见规则；SessionServer 的内存状态不能成为第二个请求权威，Ray ObjectRef 不能代替 ArtifactRef |
| 进程放置：`xtuner/v1/rl/utils/ray_accelerator_worker.py`、`ray_utils.py` | 复用 GPU/rank/bundle 映射、分布式初始化、从已有 placement group 启动 worker | detached 名称/namespace、持久 binding 与独立清理；不照搬默认建组与退出清理 |
| 指标与 trace：`xtuner/v1/_writer/`、`xtuner/v1/rl/trace/otel_utils.py`、`xtuner/v1/profiler/cuda_profile.py`、`xtuner/v1/utils/internal_metrics.py` | 复用 TensorBoard/JSONL writer、OpenTelemetry、torch profiler | 统一 run/request/transaction 身份及业务指标（空闲归因、版本、有效消费） |
| 评测指标：`xtuner/v1/rl/evaluator.py` | 指标函数复用 | 固定版本、完整任务分母、失败与未评分单列；旧 evaluator 只对传入样本求平均 |

`xtuner/v1/rl/replay_buffer.py` 的批次出入队不等价于 Experience 的持久预留与使用语义，只借鉴索引与容器实现。

### 读码事实

| 方面 | 当前 pin 的行为 | Odyssey 处理 |
| --- | --- | --- |
| 外部驱动训练 | `WorkerConfig.build` → `TrainingController`，提供 fit/offload/onload/weight_update/save/resume；`fit` 内部串联 critic value、GAE、critic 更新与 actor 更新 | 需要“冻结目标后只训练”的入口；先评估现有 worker 方法，不足则开 XTuner topic |
| 更新回执 | `step_optimizer` 在 grad_norm 非有限或超过 `skip_grad_norm_threshold` 时跳过，只返回 grad_norm，无 step 身份或幂等键 | rank 更新账本（架构 §4.4） |
| 完整状态 | DCP 保存模型与 optimizer，scheduler 另存；不保存 RNG | adapter 补 RNG 并纳入共同 manifest |
| critic 布局 | `CriticWorker` 与 actor 同进程、同 placement group，按角色切换并可 offload 分时 | 默认两模型同驻 HBM；offload 作为小 T 组的配置项 |
| 组重构 | 进程组在 actor 初始化时建立一次，只支持同规模的 NCCL suspend/resume；DCP 跨 tp/ep 重分片只有单张量、固定 world size 的测试 | T 规模变化须重启 worker 并从分片状态重载；跨 DP/EP 的 optimizer 重载是早期探测项 |
| 权重发布 | NCCL 路径由训练 rank 0 向全部推理 rank 广播，其他训练 rank 在 gloo barrier 等待；checkpoint-engine 路径每个训练 rank 持有一份 `ParameterServer` 分片并行发送；均不携带版本 | 目标规模使用 checkpoint-engine 路径；adapter 附加 publication_id 并逐 server 核对。该 ParameterServer 也是研究候选“主机内存参数服务”的起点 |
| logprob | 训练 logprob 不施加温度；rollout adapter（`xtuner/v1/rl/rollout/lmdeploy.py`）会透传 `repetition_penalty` 与 `min_tokens` | 训练 rollout 不启用任何改变分布的采样变换（架构 §3.2） |
| 模型与并行 | qwen3_5_moe 文本模型、gated deltanet、MTP 有入口；有 FSDP/TP/EP/SP，无 PP | 目标布局在 FSDP/EP/TP/SP 内选择 |
| 前缀复用 | SessionServer 按渲染后的 prompt 文本在前缀树中匹配，命中部分拼接已有节点实际 token，只对未命中部分编码；专家记录按前缀/新增/输出三段切分，前缀沿用首次记录 | gateway 复用前缀匹配（架构 §4.5）；专家记录不直接沿用首次记录，按架构 §5.1 的 `exact`/`approximate` 规则处理。PR #2221（inline system 与工具参数顺序规范化）提高命中率 |
| agent loop 钩子 | PR #2135 提供 shutdown 时关闭与 pause/resume 钩子 | Execution 暂停与发布对齐时复用 |

### 需要适配的算法差异

| 当前实现 | Odyssey 的规定 |
| --- | --- |
| `pg_loss_fn` 的 `vanilla` 含负优势 dual clip（`clip_ratio_c`，默认 3.0）与 log-ratio clamp | 基准是 `proximal_clip_tis`：PPO clip 加停止梯度的截断行为校正，不叠加 dual clip；用现有注册入口提供该函数 |
| `mask_pg` 用裁剪后的比值及单侧 mask；rollout IS 还可启用 mask/veto | 只采用声明的逐 token 截断校正，不引入额外拒收或归一化 |
| `action_gae` 从末尾 `next_value=0` 递推 | 真实终止沿用；预算截断用边界 V_old(next_state) bootstrap，credit_scope 边界切断递推；由目标构造层补充，独立 CPU 参考验证 |

## LMDeploy

### 可复用入口

| 能力与入口 | 复用方式 | Odyssey 补充 |
| --- | --- | --- |
| 推理执行、batching 与 token-in 生成：PyTorch 引擎、`/generate` 的 input_ids | 直接复用 | 稳定 request、持久 cursor 与跨版本路由 |
| KV、混合注意力状态与前缀缓存：`lmdeploy/pytorch/engine/cache_engine/`、`lmdeploy/pytorch/paging/scheduler.py`、`lmdeploy/pytorch/paging/block_trie/` | 直接复用 CacheEngine、StateCacheEngine、state slots 与前缀生命周期 | 发布时的缓存失效与实例排空；不另写 KV 分配器或状态池 |
| PD 搬运：`lmdeploy/pytorch/engine/cache_engine/migration.py`、`lmdeploy/pytorch/disagg/` | 复用缓存池描述、布局校验、字节区间映射与 RDMA 后端 | LMDeploy topic（架构 §8.2）；发布配对与重算接续归 Serving |
| 权重安装：`/update_weights`（IPC）、NCCL、checkpoint-engine | 复用 | publication_id 配对与逐 server 核对 |

### 读码事实

| 方面 | 当前 pin 的行为 | 影响与处理 |
| --- | --- | --- |
| qwen3_5_moe 推理 | 模型、线性注意力 conv/recurrent 状态、MTP 投机解码、routed experts 输出均有入口 | 单引擎 rollout 的起点 |
| PD 状态迁移 | DLSlime/Mooncake，实际只有 RDMA 可用；PD 只迁移 `CacheEngine.gpu_allocation.pools`，线性注意力状态在 `StateCacheEngine` 的独立 allocation 中，不迁移；MTP draft KV 也不迁移；没有拒绝此类模型的检查 | 混合线性注意力模型直接开 PD 会用缺失状态解码；topic 补齐前 Serving capability 拒绝该组合 |
| PD 前缀缓存 | 角色不是 Hybrid 时强制关闭前缀缓存（`executor/base.py`）；上游 main（7998cdba）仍如此 | 多轮长任务每轮重新 prefill 全部上下文；P 侧前缀缓存列为 topic 第 1 项 |
| PD 下的轨迹 | 首个 token 来自迁移步骤，没有 logprob；P 侧 routed experts 不随迁移传递 | 补齐前 PD 路径不产生训练数据 |
| PD 代理 | proxy 只代理 chat/completions，不代理 token-in 的 `/generate`；PD 下拒绝 n>1；可运行时增加节点，删除节点不排空、不中止、不重路由 | gateway 直接管理 P/D 配对与请求迁移 |
| D→D | 无 | 使用重算接续 |
| 权重更新 | IPC 路径须先 sleep 并释放 KV；逐 server 执行，没有版本字段，IPC 无返回值；更新不清空前缀缓存。上游 #5035 新增 sleep 时重置本地前缀缓存，挪 pin 时复核 | 发布时显式清空前缀缓存并以测试证明 |
| MTP draft 权重 | `update_params` 只对 `qwen3_5_mtp` 拆分并更新 draft 权重，其他方法（含 GLM-5.3 的 deepseek_mtp）的 draft 权重被静默丢弃 | 开启 GLM-5.3 投机解码前在 topic 中补齐 |
| sleep | level 1/2 释放 KV 与状态缓存，拒绝在途响应并结束全部 session | 先由 Serving 提交并挂起请求 |
| 接续 | abort 需 `--enable-abort-handling`，返回已生成尾部 | token 重算接续有入口 |
| logprob | `logprobs_mode` 只有 raw_logits/raw_logprobs，在 `logits_process.py` 的 `__call__` 开头取得，之后依次执行 guided decoding、自定义 logits processor、repetition penalty、repetition n-gram、温度、bad words、`ignore_eos`/`min_new_tokens` 期间的 stop token 屏蔽，采样阶段还有 top-k/top-p/min-p | 训练请求按最终引擎配置白名单校验（架构 §3.2） |
| 采样配置补齐 | `async_engine._determine_gen_config`：`bad_words` 并入 `bad_token_ids`，补入模型 stop token；`do_sample=False` 时改写 `top_k=1`、`temperature=1`、`repetition_penalty=1` | 白名单检查补齐与转换之后的配置 |

## 目标模型 GLM-5.3

| 方面 | 当前 pin 的行为 | 处理 |
| --- | --- | --- |
| 命名 | XTuner 的 `glm53` 代码对应 GLM-5.3-Flash（45 层，KDA 线性注意力 + NoPE-DSA），本模型走 `xtuner/v1/model/moe/glm52/` 的 `Glm52MoE`；LMDeploy 走 `glm_moe_dsa` | 按 model_type 识别 |
| IndexShare | XTuner `glm52/dsa_topk_sharing.py`，LMDeploy `glm_moe_dsa_mtp.py` 的 `index_share_for_mtp_iteration` | 已实现，验证即可 |
| 训练 | HF 加载与 MTP 键映射有入口；训练用稀疏注意力，DSA indexer 冻结；注意力无 TP，只有专家 TP，无 PP | 缩减层数配置作为早期探测起点 |
| critic | `head_type="value_head"` 与 MTP 互斥（`xtuner/v1/model/moe/moe.py`） | critic 配置显式关闭 MTP |
| 训推一致性 | MoE routing replay 读码支持（推理返回 78 层 × top-8）；DSA top-k 不回放：推理用 FP8 indexer，训练重新计算 | DSA top-k 回放按偏差实测决定优先级 |
| 推理状态与 PD | MLA latent cache 与 DSA indexer K cache 都在 `gpu_allocation.pools` 中，PD 迁移包含二者 | 只需验证；首 token logprob 与前缀缓存缺口同 qwen3_5_moe |
| 权重更新 | bf16 权重在加载时可重量化为 FP8；MTP draft 权重不更新（见上） | 推理默认 FP8；首版 PD 关闭投机解码 |
| 参考配置 | `autotest/config/rl_glm5p2_30B_gsm8k_grpo.py`、`rl_glm5p2_30B_gsm8k_dapo.py`、`rl_glm5p2_fp8_mtp4_resume.py`，按 `MODEL_PATH` 加载 | 结构探测、参数映射与恢复测试的起点，不是 753B 的通过证据 |

## Execution、评测与子 Agent

| 能力与入口 | 复用方式 | Odyssey 补充 |
| --- | --- | --- |
| 任务与沙盒：sandbox_rl `src/sandbox_rl/harbor/provider.py`、`client.py`、`environment.py`、`dataset.py`；Lagent `lagent/serving/sandbox/providers/` | 提取薄适配，继续调用 Harbor 原生任务结构与 Flotilla 环境接口 | execution/attempt 绑定、资源登记与契约转换；不另建容器编排或任务格式 |
| harness、评分与产物：sandbox_rl `src/sandbox_rl/agents/*/harness.py`、`src/sandbox_rl/rollout/execution/runner.py`、`src/sandbox_rl/harbor/verifier.py`、`recipe.py` | 提取 harness/runner；评分用 Harbor 的 `VerifierFactory` 与 `ArtifactHandler`，保留原生奖励与错误分类 | Assessment 身份、隔离与清理对账；训练 rollout 与独立 eval 共用 |
| 活任务暂停：sandbox_rl `src/sandbox_rl/agents/request_gate.py`、`runtime.py`、`src/sandbox_rl/rollout/partial.py` | 提取请求闸门、进程/session 保活和同 owner 接续 | 安全点与 ContinuationRef；进程内保活不代表 owner 崩溃可恢复 |
| 轨迹对齐：sandbox_rl `src/sandbox_rl/rollout/training_trace.py` | 借鉴 token/label/logprob 校验与历史版本投影 | 以请求树与专家记录判断身份 |
| 子 Agent 与压缩：Lagent `lagent/services/agent.py` 的 `AgentService`、`lagent/agents/compact_agent.py`、`lagent/serving/sandbox/agent.py` | Lagent harness 复用 spawn/resume/stop、状态接口与上下文压缩；其他 harness 沿用自己的子 Agent 能力 | 登记父子 execution、credit_scope、总预算与清理；压缩写入 ContextRecord |

sandbox_rl 的 Harbor 数据准备器对多步骤任务有限制，这是适配边界，不自动成为 Odyssey 所有任务的限制。

## 存储与基础设施

| 能力 | 复用方式 | Odyssey 补充 |
| --- | --- | --- |
| 元数据事务 | 集群部署的 PostgreSQL，标准驱动、连接池与迁移工具 | 领域事务、产物引用与 GC 规则 |
| 对象存储 | Petrel SDK（`petrel_client.client.Client`）：`put/get`（支持字节范围）/`delete/list/contains/isdir/size`；无追加写；默认分块上传 5 MB × 10000，即单对象上限 50 GB；配置文件 `petreloss.conf` | ArtifactStore 的 petrel 后端；DCP 对象存储 StorageWriter |
| 文件存储 | GPFS | ArtifactStore 的 gpfs 后端与 run 目录 |
| RJob 平台 | 常规任务只支持 `--restart-policy never`；自愈 `--enable-self-health`、`--self-health-count`、`--grace-period-minutes`、`--enable-auto-replace-node`；`--health-check all_group_reduce`；`--share-host-shm`（checkpoint 暂存必需）；`--store-host-nvme`；SIGTERM 后默认 60 s 优雅关停 | 提交入口与 R2 自动恢复（架构 §9.3）。一个 pod 失败的作业行为与自愈粒度待探测 |

## 可借鉴的测试

XTuner：`tests/rl/test_pack.py`、`tests/loss/test_grpo_loss.py`、`tests/rl/test_ppo_role_wiring.py`、`tests/rl/test_weight_iterator.py`、`tests/rl/test_scheduler_checkpoint.py`、`tests/rl/test_session_server_trace.py`、`tests/rl/test_trace_store.py`、`tests/rl/test_routed_experts_dtype.py`、`tests/rl/test_rollout_to_train_item.py`。sandbox_rl：`tests/unit/test_flotilla_gate.py`、`tests/unit/test_partial_tokens.py`、`tests/unit/test_harbor_eval.py`。测试代码存在不表示已在 Odyssey 环境运行通过；参考测试按实际语义适配。
