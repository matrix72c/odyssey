# Odyssey 开发流程

本文规定 odyssey 仓库的 issue → PR → 合入流程、模型分工与验证环境。后端 fork（xtuner、lmdeploy、lagent）的分支纪律仍以父工作区 AGENTS.md 为准。

## 1 仓库与权限

- 远端为 GitHub `matrix72c/odyssey`（private），人与 agent 共用 `matrix72c` 账号。
- main 分支保护：只能经 PR 合入；必需检查 `lint` 与 `gpu-tests`；squash 合入，合入后删除分支。
- 共用账号下 GitHub 的人工批准门槛不可用（作者不能 approve 自己的 PR），人工门槛由两条约定承担：
  - 人工路径：`contracts/`、composition、公共配置与 fixture、依赖声明（`pyproject.toml`、`uv.lock`）、设计文档（`docs/` 下除 Plan.md 以外）、`AGENTS.md`、`.github/`、`.claude/`、`scripts/`。改动这些路径的 PR 只能由人合入；Claude Code 的 PreToolUse hook 在 `gh pr merge` 前检查改动文件，命中即拒绝。具体路径以 `.claude/hooks/guard-merge.sh` 为准，随 TASK-00 调整。
  - issue 带 `ready` label 后才可开工，label 由人添加。

## 2 角色

| 角色 | 承担者 | 职责 |
| --- | --- | --- |
| 主控 | `claude-opus-5-5`（high；写契约、拆分任务、裁决冲突时临时调高） | 写 issue、建分支与 worktree、审查、小幅整理、合入不涉及人工路径的 PR |
| 实现 | `gpt-6-astra(high)` | 按 issue 写实现 |
| 测试 | `glm-5.3(max)[1m]` | 只依据 issue 与设计文档写测试和参考实现，不读实现代码 |
| 人 | `matrix72c` 及后续协作者 | 见第 6 节 |

agent 定义在 `.claude/agents/`，`model` 字段写完整模型名，不写 `sonnet`/`haiku` 别名：别名映射只存在于个人 shell 配置，且 `haiku` 位还承载 Claude Code 自身的后台任务。

实现者不修改 `tests/`；测试者不读实现。只有主控和人可以合入。

## 3 issue

一个 issue 是 Plan.md 某任务下一组能独立验收的 AC 切片，预计 diff 数百行，一次能审完。标题为 `TASK-xx: <切片>`，加 `task-xx` label。正文就是实现与测试的唯一输入：

```markdown
## 目标
一两句话，对应哪些 AC 的哪部分。
## 范围
允许新增或修改的文件。
## 接口
函数、类签名与语义；契约代码已存在时引用路径。
## 验收
测试要覆盖的行为与小样本预期值。
## 不做
明确排除的内容。
## 依赖
前置 issue / PR。
```

实现者需要范围之外的文件、依赖、抽象或配置项时，停下并在 issue 中提出，由主控修改 issue 后再继续。

## 4 分支与 worktree

- 分支名 `feat/<简述>`、`fix/<简述>`，从 `origin/main` 开叉；一个 issue 对应一个分支、一个 PR。
- worktree 放在 `exploit/odyssey-wt/<简述>`（GPFS 上，GPU 机可见），用 `git worktree add` 创建，`git worktree remove` 清理。
- 依赖未合入的前置 PR 时，从前置分支开叉，PR base 设为前置分支。前置 PR squash 合入后，GitHub 会把 base 改回 main，此时执行 `git rebase --onto origin/main <前置分支> <本分支>` 去掉前置提交，再 `git push --force-with-lease`。

## 5 单个 issue 的流程

1. 主控开 issue；人添加 `ready` 后，主控建分支和 worktree。
2. 测试者在该 worktree 按 issue 写测试并提交。测试先于实现，保证它只来自 issue。
3. 实现者在同一 worktree 写实现，直到测试通过。认为测试有误时在 PR 中说明理由，不自行修改；由主控裁决，测试者修正。
4. 实现者推送并开 draft PR（正文含 `Closes #N`），执行 `scripts/remote-test --status` 写入 `gpu-tests` 状态后转为 ready。
5. 主控执行 `/code-review --comment` 留下行内意见；实现者沿用同一 agent 上下文修改。小的清理由主控直接提交。
6. 不涉及人工路径时，主控执行 `gh pr merge --squash --auto`；涉及时由人审查并合入。
7. 合入后移除 worktree。

任何一步出现第 6 节的升级情形，主控给 PR 加 `needs-human` 并暂停该 PR，其余 PR 照常推进。

## 6 人工介入

人不逐个审 PR。意图偏移主要发生在设计到任务、任务到接口的翻译上，人工集中在这些位置：

| 介入点 | 时机 | 人做什么 | 是否阻塞 |
| --- | --- | --- | --- |
| issue 审定 | 每个验收波次开始前 | 批量审阅本波 issue 的范围、接口与「不做」，同意的加 `ready` | 每波一次 |
| 人工路径 PR | 改动第 1 节人工路径时 | 审查并合入 | 下游 issue 可从该 PR 分支开叉继续，要求修改时 rebase |
| 升级 | PR 与 issue 有偏差、测试被判有误、需要 issue 范围外改动、主控无法裁决审查分歧 | 处理 `needs-human` 的 PR | 只暂停该 PR |
| 波次验收 | 一波全部合入后 | 对照 Plan.md AC 状态与证据验收；抽查已合入 PR；反复出现的问题写回审查清单和 issue 模板 | 下一波 issue 可同时准备 |

TASK-00 与第一个波次的 PR 全部由人审查合入，用于校准主控的审查标准与 issue 写法，之后按上表执行。

## 7 审查清单

- 只修改了 issue 允许的文件；实现提交没有改动 `tests/`。
- 校验只发生在外部数据边界，不重复检查 schema、构造函数或内部契约已保证的条件。
- 没有单次使用的抽象、预留参数或为未出现需求准备的扩展点。
- 只在能恢复、补充上下文或转换边界错误时捕获异常。
- 并发保护与故障处理都能说出实际失败场景。
- 测试检验行为而非复制实现；mock 只用于外部依赖。

每条审查意见须附具体失败场景或违反的条款，“这样更健壮”不构成意见。反复出现的问题写回 issue 模板的「不做」或本节。

## 8 验证环境

### 8.1 机器分工

| 机器 | 用途 | 限制 |
| --- | --- | --- |
| 开发机 | 编辑、git、`gh`、ruff、`uv sync` | 7 核 15 GB，根分区余量小；不跑测试、不编译 |
| GPU 机（父 AGENTS.md 中的常驻机） | 所有 pytest 与编译 | 8×H200，无外网；默认 rootfs 无 nvcc 与 PostgreSQL |

Claude Code 项目设置禁止在开发机直接运行 pytest，测试只经 `scripts/remote-test`。GPU 机是 rjob pod，失效时按父 AGENTS.md 的分区规则重新申请同规格机器，并更新 `scripts/remote-test` 中的主机名。

### 8.2 环境

- Python 解释器与 uv cache 放在 GPFS 上（`UV_PYTHON_INSTALL_DIR`、`UV_CACHE_DIR`），因为两台机器的 `/home` 不共享。
- 开发机执行 `uv sync` 生成 `odyssey/.venv`，所有 worktree 共用。`remote-test` 把 worktree 的源码目录放在 `PYTHONPATH` 最前，使导入解析到该 worktree。修改依赖的 PR 在自己的 worktree 中执行 `uv sync` 生成 `<worktree>/.venv`，`remote-test` 优先使用它。
- PostgreSQL 二进制随 venv 提供，由测试 fixture 每次运行起临时实例；具体方案在 TASK-00 确定。

### 8.3 `scripts/remote-test`

`scripts/remote-test [--gpus N] [--status] [pytest 参数]`，在当前 worktree 执行：

- 经 ssh 在 GPU 机的该 worktree 下用共享 venv 运行 pytest。
- 按 `--gpus` 独占分配空闲卡，默认不分配（`CUDA_VISIBLE_DEVICES` 置空）；每次运行使用独立的临时目录、PostgreSQL 实例和 Ray 端口，使并行的 PR 互不干扰。
- 日志写入 `odyssey/.test-logs/`（gitignore）。
- `--status` 只在工作区干净且 HEAD 已推送时可用，忽略自定义 pytest 参数，固定运行门槛集合，再由开发机用 `gh api` 给 HEAD 写 `gpu-tests` 状态，附通过/失败数与日志路径。新提交推送后旧状态自然失效。

门槛集合为 `tests/contracts/` 与 `tests/numerics/`；PR 涉及后端适配时加上 `tests/backend/`。`tests/e2e/` 与多节点场景不进入门槛，证据记录在 Plan.md 第 7 节。

### 8.4 GitHub Actions

只运行 `lint` 检查（`ruff check` 与 `ruff format --check`），不跑测试。

## 9 PR

标题与 issue 相同。正文只有四项：

```markdown
Closes #N
## 与 issue 的偏差
## 验证
实际运行的命令与结果。
## 需要人看的点
```

不写复述 diff 的总结。完成某条 AC 的 PR 同时修改 Plan.md 第 7 节对应行，证据填测试命令或产物路径。

提交作者只用提交人自己的 git 身份。提交信息不加 `Co-Authored-By: Claude …` 等 AI 署名行，PR 正文不加生成工具标记。
