#!/usr/bin/env bash
# PreToolUse hook：改动人工路径的 PR 不允许由 agent 执行 gh pr merge。
# 人工路径的定义以本文件为准，见 docs/Development_Workflow.md 第 1 节。
set -euo pipefail

command=$(jq -r '.tool_input.command // ""')
[[ $command =~ gh[[:space:]]+pr[[:space:]]+merge ]] || exit 0

if [[ ! $command =~ gh[[:space:]]+pr[[:space:]]+merge[[:space:]]+([0-9]+) ]]; then
  echo "gh pr merge 必须紧跟 PR 号，以便检查改动路径。" >&2
  exit 2
fi
pr=${BASH_REMATCH[1]}

if ! files=$(gh pr diff "$pr" --name-only); then
  echo "无法读取 PR #$pr 的改动文件，拒绝合入。" >&2
  exit 2
fi

human_paths='^(src/odyssey/(contracts|config|cli)/|tests/conftest\.py$|tests/fixtures/|pyproject\.toml$|uv\.lock$|docs/|AGENTS\.md$|\.github/|\.claude/|scripts/)'
hits=$(grep -vx 'docs/Plan.md' <<<"$files" | grep -E "$human_paths" || true)
if [[ -n $hits ]]; then
  printf 'PR #%s 改动了人工路径，只能由人合入：\n%s\n' "$pr" "$hits" >&2
  exit 2
fi
